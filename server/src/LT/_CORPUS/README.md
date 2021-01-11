- ID_FULLTEXT.tsv is the mapping from article id (sequence) to full body text.
- FINAL.tsv is extracted from the Excel file of annotations.
- Corpus.tsv is produced from the above two files, using the 'SimplifyCorpus.py' script.

You could re-create the Corpus by running
./SimplifyCorpus.py ID_FULLTEXT.tsv FINAL.tsv Corpus.tsv
