
from helpers import logger, pipelinefn
import helpers
from urllib.parse import urljoin
import wget
import bz2
import gensim.corporas
import os
import re
import toolz
import xmltodict
import pandas as pd
import sqlite3


DUMPS_BASE_URL = 'https://dumps.wikimedia.org/enwiki/'
LARGE_DUMP_PATH_TEMPLATE = '{date}/enwiki-{date}-pages-articles-multistream.xml.bz2'
SMALL_DUMP_PATH = '20250620/enwiki-20250620-pages-articles-multistream1.xml-p1p41242.bz2'


@pipelinefn
def get_dump_url(mode):

    if mode not in ('small', 'large', 'latest'):
        raise NotImplementedError('Unkwown mode \'%s\'' % mode)
    
    if mode == 'latest':
        raise NotImplementedError('Mode *%s* needs to be implemented.' % mode)

    dump_path = (LARGE_DUMP_PATH_TEMPLATE.format(date='20250620') if mode == 'large'
                 else SMALL_DUMP_PATH)
    url = urljoin(DUMPS_BASE_URL, dump_path)
    return url


@pipelinefn
def download_dump(url):
    logger.info('....... Downloading dump from %s' % url)
    local_filename = "scratch/wikidump.xml.bz2"
    if os.path.exists(local_filename):
            logger.info("{} exists, skipping".format(local_filename))
    else:
        wget.download(url, out=local_filename)
    if not os.path.exists(local_filename):
        raise NotImplementedError()
    return local_filename


@pipelinefn
def extract_pages(dump_path):
    logger.info('....... Extracting pages from dump %s' % dump_path)
    extract_path = 'pages/'
    if not os.path.exists("pages"):
        os.mkdir("pages")

    # Counters
    pagecount = 0
    filecount = 1

    # Open chunkfile in write mode
    pageid = lambda filecount: os.path.join("pages","page-"+str(filecount)+".xml")
    pagefile = open(pageid(filecount), 'w')
    bz2open = bz2.BZ2File(dump_path,'r')
    found_start=False
    
    for line in bz2open:

        # start parsing at first page not wikipedia metadata
        if not found_start:
            if b'<page>' in line:
                    found_start = True
        if found_start:
            line = line.decode('utf-8')
            
            pagefile.write(line)
            # the </page> determines new wiki page
            if '</page>' in line:
                pagecount += 1
            if pagecount >= 1:
                pagefile.close()
                pagecount = 0 # RESET pagecount
                filecount += 1 # increment filename
                
                # iterate on pageid generator object
                pagefile = open(pageid(filecount), 'w')
            
            # just for dev
            if filecount > 100:
                break
    try:
        pagefile.close()
    except:
        logger.info('Files already closed')
    return extract_path
    raise NotImplementedError()


def compute_frequencies_multiprocessing_wrapped(pagefile:str):
    filepath = os.path.join('pages/', pagefile)
    with open(filepath, 'rt') as f:
        extractpage = f.read()

    # transform xml to dict
    if len(extractpage) == 0:
        return None
    extractpagedict = xmltodict.parse(extractpage)

    # regex transform all text to lowercase
    text = extractpagedict['page']['revision']['text']['#text']
    text = re.sub(r"[^a-zA-Z0-9 ']", ' ', text).lower()
    
    # remove pages with redirect in text
    if text.startswith('redirect'):
        return None
    
    # reformat title
    title = extractpagedict['page']['title'].replace(' ','_')

    # remove pages with ignored namespaces in title
    if any(namespace in title for namespace in gensim.corpora.wikicorpus.IGNORED_NAMESPACES):
        return None
    
    # remove words not to be counted in frequency
    textlist = text.split()
    textlist = [word for word in textlist if word not in helpers.STOPWORDS]
    
    # create frequencies and return columns
    freqs = toolz.itertoolz.frequencies(textlist)
    words = freqs.keys()
    wordfreqs = freqs.values()
    titlelist = [title] * len(freqs)

    # initialize pandas df
    pagedf = pd.DataFrame(data={
        'wikipedia_page_id':titlelist,
        'word':words,
        'frequency': wordfreqs,
        'relative_frequency': [x / sum(list(wordfreqs)) for x in wordfreqs]
    })
    return pagedf


@pipelinefn
def compute_frequencies(extract_path):
    logger.info('....... Computing frequency tables from %s' % extract_path)
    contents = os.listdir(extract_path)

    # initialize pandas df
    df = pd.DataFrame(data={
        'wikipedia_page_id':[],
        'word':[],
        'frequency':[],
        'relative_frequency':[]
    })

    for page in contents:
        if compute_frequencies_multiprocessing_wrapped(page) is None:
            pass
        else:
            # add title to title col, explode per num of words in freqs
            pagedf = compute_frequencies_multiprocessing_wrapped(page)
            df = pd.concat([df, pagedf])
    
    # aggregate frequencies for new columns
    df['word_page_count'] = df.groupby('word')['wikipedia_page_id'].transform('count')
    df['word_overall_frequency'] = df.groupby('word')['frequency'].transform('sum')

    # removing words whose total frequency over the whole set of pages is too low
    # removing words that appear in too few documents
    df = df[df['word_overall_frequency'] >=5]
    df = df[4 <= df['word_page_count']]
    halfnumberpages = (len(df['wikipedia_page_id'].unique()) // 2)
    df = df[df['word_page_count'] <= halfnumberpages]

    # df.to_csv('wikidump_word_frequency.csv', index=False)
    return df
    raise NotImplementedError()


@pipelinefn
def push_to_db(input):
    logger.info('....... Pushing data to sqlite db')
    conn = sqlite3.connect('scratch/wikidump_word_frequency.db')
    table_name = 'wikidump_word_frequency'
    input.to_sql(table_name, conn, if_exists='replace', index=False)
    return None
    raise NotImplementedError()


# This is the SQL query that takes the top 25 words from the Wiki overall that appear in a given page --------
"""
SELECT t.wikipedia_page_id, t.word, t.tf * i.idf AS tf_idf_score
FROM
    (SELECT
        wikipedia_page_id,
        word,
        (COUNT(word) * 1.0) / SUM(COUNT(word)) OVER (PARTITION BY wikipedia_page_id) AS tf
    FROM
        wikidump_word_frequency
    GROUP BY
        wikipedia_page_id, word) AS t
JOIN
    (SELECT
        word,
        LOG(CAST(total_documents AS DECIMAL) / COUNT(DISTINCT wikipedia_page_id)) AS idf
    FROM
        wikidump_word_frequency,
        (SELECT COUNT(DISTINCT wikipedia_page_id) AS total_documents FROM wikidump_word_frequency) AS doc_counts
    GROUP BY
        word, total_documents) AS i
ON
    t.word = i.word;
"""

"""
SELECT word, tf_idf_score 
FROM wikidump_word_frequency
WHERE wikipedia_page_id = 'Anarchism'
ORDER BY tf_idf_score DESC
LIMIT 25;
"""
