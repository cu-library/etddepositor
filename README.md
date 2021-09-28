# etddepositor

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

`etddepositor` is a command line tool written in Python 3 to facilitate loading Carleton University
electronic theses and dissertations (ETDs) into a Hyrax-powered institutional repository.

The depositor transfers ETD packages in BagIt format from a shared storage directory, validates and transforms ETD metadata,
and calls the Bulkrax ingest tool.
