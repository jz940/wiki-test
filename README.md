# Processing Wikipedia pages
## Pipeline

This project provides a skeleton for a pipeline that downloads a MediaWiki dump from the online
[repository](https://dumps.wikimedia.org/enwiki/), extracts the Wikipedia pages from the dump and creates
a database of word frequencies.

The pipeline consists of a series of steps; each step has a corresponding function in the
file `pipeline.py`. The pipeline can be run using the file `run.py`, each function will be
called with one argument that will point to the output of the preceding function; for example,
the first function `get_dump_url` will return a URL that will be passed to the second function
`download_dump`. If an exception is raised in any of the functions, the pipeline execution will
log the error and will not execute any function after that point. Please, run `run.py` before
starting to get a better idea of how the pipeline is run. Feel free to change the steps in the
pipeline if you wish to partition the work differently.

Once the pipeline functions have been implemented, a successful execution of the pipeline should
result in a `sqlite` database wich will contain at least one table with the following columns:

- Wikipedia page ID: An identifier for the page, generated from the title with spaces replaced by underscores.
- Word: Any of the words appearing in the set of Wikipedia pages.
- Frequency: Number of times the word appears in the page.
- Relative frequency: The frequency divided by the total number of word occurrences in the page (the same word
  occurring multiple times is counted as many times as it occurs).
  
Ignoring the following:

- Wikipedia pages whose wiki-text starts with '#REDIRECT', they are just pointers to an actual page.
- Wikipedia pages with a title starting with `<namespace>:` for the namespaces included in
`gensim.corpora.wikicorpus.IGNORED_NAMESPACES`.
- Words that are stopwords (see `helpers.py`).
- Words whose total frequency over the whole set of pages is too low (e.g. less than 5 occurrences).
- Words that appear in too few documents (e.g. less than 4) or in too many (more than 50% of the documents).

Since the number of total page-word combinations can grow quickly and become huge, you can limit the words counted
to a subset (vocabulary) with the most frequent words in the dataset. Another option is processing only the first N
documents in the dump during development if the time it takes to run the pipeline makes development too slow. Please
note that while you can work with a smaller set of documents during development, the pipeline still should be able to
process the whole dataset once finished.

## SQL query

Will write a SQL query that given a Wikipedia page returns the 25 most relevant words for that page using the `sqlite`
database. The relevance of a word in a page can be scored dividing the frequency (or relative frequency) of the word in
the document by the logarithm `log(Nd/n)`, where `Nd` is the total number of documents and `n` the number of documents
in which the word appears. This scoring method is known as TF-IDF.

## If there is time later

Considering changing the `get_dump_url` function to parse the [entry page](https://dumps.wikimedia.org/enwiki/) of the
repository to automatically select the latest completed dump; completed dumps are those inside a directory marked
with a date (directory `latest` might contain incomplete data).

Reducing the huge amount of memory the pandas df constructed in the compute_frequencies function can improve efficiency in future.