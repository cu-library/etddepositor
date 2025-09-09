# etddepositor

This script automates the ingest of Carleton University ETD (Electronic Theses and Dissertations) packages into a DSpace repository. It validates, processes, and imports ETD packages, generates MARC and Crossref metadata, creates postback files, and sends a summary email report.

## Features

- **BagIt Validation:** Ensures ETD packages are valid before processing.
- **Metadata Extraction:** Parses XML metadata and applies mappings for degree, discipline, subject, etc.
- **File Handling:** Copies thesis PDFs and supplemental files, and attaches required license documents.
- **DSpace API Integration:** Creates items, bundles, and uploads files using the DSpace REST API.
- **MARC & Crossref Metadata:** Generates MARC records and Crossref XML for each thesis.
- **Reporting:** Writes postback files for ITS, creates CSV ingest reports, and sends summary emails with attachments.

## Usage

Run the script from the command line:

python [etddepositor.py] <base_directory> --api-base <API_BASE> --mapping-file <mappings.yaml> --parent-collection-id <collection_id> --user-email <email> --user-password <password> --email-from <from_addr> --email-to <to_addr> --smtp-host <smtp_host> --smtp-port <port>


## Required Arguments & Options

<base_directory>: Root directory containing ETD packages.
--api-base: DSpace API base URL (default: https://carleton-dev.scholaris.ca/server/api). **note this is set to default on dev server on purpose you must specify production for live deposit**
--mapping-file: Path to the YAML file with metadata mappings.
--parent-collection-id: DSpace collection UUID to ingest into.
--user-email, --user-password: DSpace user credentials.
--email-from, --email-to: Email addresses for report delivery.
--smtp-host, --smtp-port: SMTP server details for sending emails.
Other options: See script for additional flags (e.g., skipped mappings, outbox path, DOI start, etc.).

## Output

 -  MARC records in a zip archive.
 -  Crossref XML metadata file.
 -  CSV ingest report summarizing processed packages.
 -  Postback files for each package.
 -  Email report with attachments.

## Dependencies

 - **bagit** (https://pypi.org/project/bagit/)
 - **pymarc** (https://pypi.org/project/pymarc/)
 - **PyYAML** (https://pypi.org/project/PyYAML/)
 - **requests** (https://pypi.org/project/requests/)
 - **click** (https://pypi.org/project/click/)
 - **requests-toolbelt** (https://pypi.org/project/requests-toolbelt/)

## Notes

 - Ensure your mappings YAML files are correctly formatted.
 - The script expects specific directory structures and file naming conventions for ETD packages.
 - For more details, see the function docstrings in etddepositor.py.
 - Last known DOI (2025-16681)[Sept-09 {MR}]

## Future Features

 - Update current itteration of DSPortal code to the pylib version
 - OLRC API Integration
 - tag release
 - Permafrost Integration (low prio)
 - Re-implement linting options
 - Re-implement the unit testing we had from Hyrax

## Legacy 
 - Needs to be updated to match current linting standards. 
 
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Black Formatting](https://github.com/cu-library/etddepositor/actions/workflows/black.yml/badge.svg)](https://github.com/cu-library/etddepositor/actions/workflows/black.yml)
[![Flake8 and Unit Tests](https://github.com/cu-library/etddepositor/actions/workflows/python-package.yml/badge.svg)](https://github.com/cu-library/etddepositor/actions/workflows/python-package.yml)
