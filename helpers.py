import os
import glob
import logging
import requests
import functools
import time
import traceback


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging


def pipelinefn(f):
    fname = f.__name__
    @functools.wraps(f)
    def fn(x):
        if isinstance(x, Exception):
            return x
        logger.info('Running %s...' % fname)
        start_time = time.time()
        try:
            result = f(x)
            took = time.time() - start_time
            logger.info('....... OK (took %.1fs)' % took)
            return result
        except Exception as e:
            took = time.time() - start_time
            exc_info = ''.join(traceback.format_exception(e)[2:])
            logger.error('....... Failed after %.1fs\n%s' % (took, exc_info))
            return e
    return fn


def get_file_list(dir=None, pattern='*'):
    full_pattern = os.path.join(dir, pattern) if dir else pattern
    files = glob.glob(full_pattern)
    return files


STOPWORDS = ['a', 'about', 'above', 'after', 'again', 'against', 'ain', 'all', 'am', 'an', 'and', 'any', 'are', 'aren',
             "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by',
             'can', 'couldn', "couldn't", 'd', 'did', 'didn', "didn't", 'do', 'does', 'doesn', "doesn't", 'doing',
             'don', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', "hadn't", 'has',
             'hasn', "hasn't", 'have', 'haven', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here',
             'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into',
             'is', 'isn', "isn't", 'it', "it'd", "it'll", "it's", 'its', 'itself', 'just', 'll', 'm', 'ma', 'me',
             'mightn', "mightn't", 'more', 'most', 'mustn', "mustn't", 'my', 'myself', 'needn', "needn't", 'no', 'nor',
             'not', 'now', 'o', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out',
             'over', 'own', 're', 's', 'same', 'shan', "shan't", 'she', "she'd", "she'll", "she's", 'should',
             "should've", 'shouldn', "shouldn't", 'so', 'some', 'such', 't', 'than', 'that', "that'll", 'the', 'their',
             'theirs', 'them', 'themselves', 'then', 'there', 'these', 'they', "they'd", "they'll", "they're",
             "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 've', 'very', 'was', 'wasn',
             "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', 'weren', "weren't", 'what', 'when', 'where',
             'which', 'while', 'who', 'whom', 'why', 'will', 'with', 'won', "won't", 'wouldn', "wouldn't", 'y', 'you',
             "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves']
