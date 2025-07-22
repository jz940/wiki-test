
from helpers import logger, pipelinefn
from urllib.parse import urljoin


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
    raise NotImplementedError()
    local_filename = None
    return local_filename


@pipelinefn
def extract_pages(dump_path):
    logger.info('....... Extracting pages from dump %s' % dump_path)
    raise NotImplementedError()


@pipelinefn
def compute_frequencies(dump_path):
    raise NotImplementedError()


@pipelinefn
def push_to_db(input):
    raise NotImplementedError()

