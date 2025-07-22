
from cytoolz import pipe
from helpers import logger
from pipeline import (
    get_dump_url,
    download_dump,
    extract_pages,
    compute_frequencies,
    push_to_db
)


if __name__ == '__main__':

    logger.info('Pipeline started')

    pipe(
        get_dump_url('small'),
        download_dump,
        extract_pages,
        compute_frequencies,
        push_to_db
    )
