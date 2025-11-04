import os
import glob
from datetime import datetime, timezone
import yaml
import click
import xml.etree.ElementTree as ElementTree
import shutil
import csv
import bagit
import dataclasses
import string
from typing import List
import smtplib
from xml.etree import ElementTree
from xml.dom import minidom
import time
import pymarc
import requests
import textwrap
import mimetypes
from requests_toolbelt.multipart import encoder
import json
import hashlib


# API_BASE & Base URL supplied as an argument through click to speicfy if were on Dev or Live

# These sub-directories are made where you specify the base dir.
READY_SUBDIR = "ready"
DONE_SUBDIR = "done"
MARC_SUBDIR = "marc"
CROSSREF_SUBDIR = "crossref"
CSV_REPORT_SUBDIR = "csv_report"
FILE_SUBDIR = "file"
FAILED_SUBDIR = "failed"
SKIPPED_SUBDIR = "skipped"
LICENSE_SUBDIR = "license"
POSTBACK_SUBDIR = "postback_tmp"

# NAMESPACES is used to help pull out the data from FGPA packages
NAMESPACES = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "etdms": "http://www.ndltd.org/standards/metadata/etdms/1.1/",
}

# Bitstream Payloads are blank payloads as we'll fill them out when we place it into dspace
OG_BITSTREAM_PAYLOAD = {
    "name": "",
    "description": "",
    "type": "bitstream",
    "bundleName": "ORIGINAL",
}

LICENSE_BITSTREAM_PAYLOAD = {
    "name": "",
    "description": "",
    "type": "license",
    "bundleName": "LICENSE",
}

# PackageData create the template for this object and will be placing data from
PackageData = dataclasses.make_dataclass(
    "PackageData",
    [
        "package_files",
        "creator",
        "contributors",
        "date",
        "type",
        "description",
        "publisher",
        "doi",
        "language",
        "rights_notes",
        "title",
        "subjects",
        "abbreviation",
        "deduped_subjects",
        "agreements",
        "degree",
        "degree_discipline",
        "degree_level",
        "url",
        "handle",
        "student_id",
        "embargo_info",
    ],
)


# DOI_PREFIX is Carleton University Library's DOI prefix, used when minting new
# DOIs for ETDs.
DOI_PREFIX = "10.22215"

# DOI_URL_PREFIX is the prefix to add to DOIs to make them resolvable.
DOI_URL_PREFIX = "https://doi.org/"

# FLAG is a string which we assign to some attributes of the package
# if our mapping for that attribute is incomplete or unknowable.
FLAG = "FLAG"


class MissingFileError(Exception):
    """Raised when a required file is missing."""


class MetadataError(Exception):
    """Raised when a problem with the package metadata is encountered."""


class GetURLFailedError(Exception):
    """Raised when the Hyrax URL for an imported package can't be found."""


# DSpaceSession: This is the class itself that will handle all the DSpace API management
# All we need to do is make a session and uses the user/pass you pass in
class DSpaceSession(requests.Session):
    def __init__(self, api_base):
        super().__init__()
        self.api_base = api_base
        self.auth_token = None
        self.last_auth_time = None
        self.csrf_token = None
        self.fetch_initial_csrf_token()

    def fetch_initial_csrf_token(self):
        response = super().get(f"{self.api_base}/security/csrf")
        response.raise_for_status()
        self.headers.update(response)

    def authenticate(self, user, password):
        login_payload = {"user": user, "password": password}
        try:
            response = self.post(
                f"{self.api_base}/authn/login", data=login_payload
            )
            response.raise_for_status()
            self.update_csrf_token(response)

            self.auth_token = response.headers.get("Authorization")
            if self.auth_token:
                self.headers.update({"Authorization": self.auth_token})
                self.last_auth_time = time.time()

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error during authentication for user {user}: {e}")
            print(f"Response content: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Request error during authentication for user {user}: {e}")

        except Exception as e:
            print(
                f"Unexpected error during authentication for user {user}: {e}"
            )

    def refresh_csrf_token(self):
        response = super().get(f"{self.api_base}/security/csrf")
        response.raise_for_status()
        self.update_csrf_token(response)

    def ensure_auth_valid(self, user, password):
        if self.auth_token and (time.time() - self.last_auth_time > 1800):
            self.authenticate(user, password)

    def update_csrf_token(self, response):
        if "dspace-xsrf-token" in response.headers:
            self.csrf_token = response.headers["DSPACE-XSRF-TOKEN"]
            self.headers.update({"X-XSRF-TOKEN": self.csrf_token})

    def request(self, method, url, **kwargs):
        try:
            response = super().request(method, url, **kwargs)
            response.raise_for_status()
            self.update_csrf_token(response)
            print(f"Successful {method} request to {url}")
            return response

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error during {method} request to {url}: {e}")
            print(f"Response content: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Request error during {method} request to {url}: {e}")

        except Exception as e:
            print(f"Unexpected error during {method} request to {url}: {e}")

    def safe_request(self, method, url, **kwargs):
        return self.request(method, url, **kwargs)


# load_mappings is a helper method that will load all the yaml files we use
def load_mappings(mapping_file):
    """Loads the mappings YAML file."""
    try:
        with open(mapping_file, encoding="utf-8") as mappings_file:
            mappings = yaml.load(mappings_file, Loader=yaml.FullLoader)
        return mappings
    except FileNotFoundError:
        click.echo(f"Error: Mappings file not found at {mapping_file}")
        return None
    except yaml.YAMLError as e:
        click.echo(f"Error parsing mappings file: {e}")
        return None


def validate_subject_mappings(mappings):
    """Ensures the subjects in the mappings file are properly formatted."""
    if mappings and "lc_subject" in mappings:
        for code, subject in mappings["lc_subject"].items():
            for subject_tags in subject:
                if len(subject_tags) not in [2, 4]:
                    click.echo(
                        f"Warning: The subject {code} in the mappings file is not formatted correctly."
                    )


def find_etd_packages(processing_directory):
    """Finds the package directories in the ready subdirectory."""
    ready_path = os.path.join(processing_directory, READY_SUBDIR)
    packages = glob.glob(os.path.join(ready_path, "*"))
    return packages


def create_output_directories(processing_directory):
    """Creates the timestamped output directories."""
    done_path = os.path.join(processing_directory, DONE_SUBDIR)
    file_path = os.path.join(processing_directory, FILE_SUBDIR)
    marc_path = os.path.join(processing_directory, MARC_SUBDIR)
    crossref_path = os.path.join(processing_directory, CROSSREF_SUBDIR)
    csv_report_path = os.path.join(processing_directory, CSV_REPORT_SUBDIR)
    failed_path = os.path.join(processing_directory, FAILED_SUBDIR)
    skipped_path = os.path.join(processing_directory, SKIPPED_SUBDIR)
    license_path = os.path.join(processing_directory, LICENSE_SUBDIR)
    postback_path = os.path.join(processing_directory, POSTBACK_SUBDIR)

    os.makedirs(done_path, mode=0o770, exist_ok=True)
    os.makedirs(marc_path, mode=0o775, exist_ok=True)
    os.makedirs(crossref_path, mode=0o775, exist_ok=True)
    os.makedirs(csv_report_path, mode=0o775, exist_ok=True)
    os.makedirs(file_path, mode=0o775, exist_ok=True)
    os.makedirs(failed_path, mode=0o775, exist_ok=True)
    os.makedirs(license_path, mode=0o775, exist_ok=True)
    os.makedirs(postback_path, mode=0o775, exist_ok=True)

    return (
        done_path,
        marc_path,
        crossref_path,
        csv_report_path,
        file_path,
        failed_path,
        skipped_path,
        license_path,
        postback_path,
    )


def write_metadata_csv_header(metadata_csv_path):
    """Write the header columns to the Hyrax import metadata CSV file."""
    header_columns = [
        "files",
        "dc.contributor.author",
        "dc.contributor.other",
        "dc.date.issued",
        "dc.type",
        "dc.description.abstract",
        "dc.publisher",
        "dc.identifier.doi",
        "dc.language.iso",
        "dc.rights",
        "dc.title",
        "dc.subject.lcsh",
    ]

    with open(
        metadata_csv_path, "w", newline="", encoding="utf-8"
    ) as metadata_csv_file:
        csv_writer = csv.writer(metadata_csv_file)

        csv_writer.writerow(header_columns)


def process_subjects(subject_elements, mappings):
    subjects = []
    for subject_element in subject_elements:
        subject_code = subject_element.text.strip()

        if subject_code.endswith("."):
            subject_code = subject_code[:-1]

        if subject_code in mappings["lc_subject"]:
            for subject_tags in mappings["lc_subject"][subject_code]:
                subjects.append(subject_tags)
    deduplicated_subjects = []
    for subject in subjects:
        if subject not in deduplicated_subjects:
            deduplicated_subjects.append(subject)

    subject_values = [
        entry[1].rstrip(".")
        for entry in deduplicated_subjects
        if len(entry) > 1
    ]
    return deduplicated_subjects, subject_values


def process_description(description):
    return description.replace("\n", " ").replace("\r", "").strip()


def process_contributors(contributor_elements):
    contributors = []
    for contributor_element in contributor_elements:
        name = contributor_element.text.strip()
        role = contributor_element.get("role", "")
        if role:
            # Uppercase the first character of the role.
            role = role[0].upper() + role[1:]
            contributors.append(f"{name} ({role})")
        else:
            contributors.append(name)
    return contributors


def process_date(date):
    """Check date is properly formatted, return the date and year as strings"""

    date = date.strip()
    if not date:
        raise MetadataError("date tag is missing")
    try:
        year = str(datetime.strptime(date, "%Y-%m-%d").year)
    except ValueError:
        raise MetadataError(f"date value {date} is not properly formatted")
    return date, year


def process_language(language):
    language = language.strip()
    if language == "fre" or language == "fra":
        return "fr"
    elif language == "ger" or language == "deu":
        return "de"
    elif language == "spa":
        return "sp"
    elif language == "eng" or language == "":
        return "en"
    else:
        raise MetadataError(f"unexpected language {language} found.")


def process_degree(degree):
    degree = degree.strip()
    if degree == "Master of Architectural Stud":
        return "Master of Architectural Studies"
    elif degree == "Master of Information Tech":
        return "Master of Information Technology"
    elif degree == "":
        return FLAG
    return degree


def process_degree_abbreviation(degree, mappings):
    return mappings["abbreviation"].get(degree, FLAG)


def process_degree_discipline(discipline, mappings):
    discipline = discipline.strip()
    return mappings["discipline"].get(discipline, FLAG)


def process_degree_level(level):
    level = level.strip()
    if not level:
        raise MetadataError("degree level is missing")
    if level == "0":
        raise MetadataError("received undergraduate work, degree level is 0")
    if level != "1" and level != "2":
        raise MetadataError("invalid degree level")
    if level == "1":
        level = "Master's"
    elif level == "2":
        level = "Doctoral"
    return level


def create_package_data(
    package_metadata_xml,
    student_id,
    doi_ident,
    agreements,
    embargo_info,
    mappings,
):
    """Extract the package data from the package XML."""

    root = package_metadata_xml.getroot()

    title = root.findtext("dc:title", default="", namespaces=NAMESPACES)
    title = title.strip()
    if title == "":
        raise MetadataError("title tag is missing")

    creator = root.findtext("dc:creator", default="", namespaces=NAMESPACES)
    creator = creator.strip()
    if creator == "":
        raise MetadataError("creator tag is missing")

    subject_elements = root.findall("dc:subject", namespaces=NAMESPACES)
    deduped_subjects, subjects = process_subjects(subject_elements, mappings)

    description = root.findtext(
        "dc:description", default="", namespaces=NAMESPACES
    )
    description = process_description(description)

    publisher = root.findtext(
        "dc:publisher", default="", namespaces=NAMESPACES
    )
    publisher = publisher.strip()
    if publisher == "":
        publisher = "Carleton University"

    contributor_elements = root.findall(
        "dc:contributor", namespaces=NAMESPACES
    )
    contributors = process_contributors(contributor_elements)

    date = root.findtext("dc:date", default="", namespaces=NAMESPACES)
    date, year = process_date(date)

    language = root.findtext("dc:language", default="", namespaces=NAMESPACES)
    language = process_language(language)

    degree = root.findtext(
        "etdms:degree/etdms:name", default="", namespaces=NAMESPACES
    )
    degree = process_degree(degree)

    abbreviation = process_degree_abbreviation(degree, mappings)

    rights_notes = root.findtext(
        "dc:rights_notes", default="", namespaces=NAMESPACES
    )

    rights_notes = rights_notes.replace(rights_notes, "")
    if rights_notes == "":
        rights_notes = (
            f"Copyright © {year} the author(s). Theses may be used for "
            "non-commercial research, educational, or related academic "
            "purposes only. Such uses include personal study, distribution to "
            "students, research and scholarship. Theses may only be shared by "
            "linking to the Carleton University Institutional Repository and "
            "no part may be copied without proper attribution to the author; "
            "no part may be used for commercial purposes directly or "
            "indirectly via a for-profit platform; no adaptation or "
            "derivative works are permitted without consent from the "
            "copyright owner."
        )

    discipline = root.findtext(
        "etdms:degree/etdms:discipline", default="", namespaces=NAMESPACES
    )
    discipline = process_degree_discipline(discipline, mappings)

    level = root.findtext(
        "etdms:degree/etdms:level", default="", namespaces=NAMESPACES
    )
    level = process_degree_level(level)

    doi = f"{DOI_PREFIX}/etd/{year}-{doi_ident}"

    return PackageData(
        package_files=[],
        creator=creator,
        contributors=contributors,
        date=year,
        type="thesis",
        description=description,
        publisher=publisher,
        doi=doi,
        language=language,
        rights_notes=rights_notes,
        title=title,
        subjects=subjects,
        abbreviation=abbreviation,
        deduped_subjects=deduped_subjects,
        agreements=agreements,
        degree=degree,
        degree_discipline=discipline,
        degree_level=level,
        url="",
        handle="",
        student_id=student_id,
        embargo_info=embargo_info,
    )


def process_value(value):

    if isinstance(value, list):
        return "||".join(map(str, value))
    elif isinstance(value, str):
        return value.strip()
    else:
        return str(value)


def copy_thesis_pdf(package_data, package_path, files_path):
    # ASSUMPTION: The file main thesis will always be a .pdf file.
    file_paths_in_data = glob.glob(os.path.join(package_path, "data", "*pdf"))

    largest_file_size = 0
    thesis_file_path = None

    # Because the files names are not consistent, get
    # the largest file ending in .pdf. Not foolproof.
    for potential_file_path in file_paths_in_data:
        size = os.path.getsize(potential_file_path)
        if size > largest_file_size:
            thesis_file_path = potential_file_path
            largest_file_size = size

    if not thesis_file_path:
        raise MetadataError("could not find pdf file")

    # We want an short pdf file name.
    # The first part is the creator name, simplified.
    dest_file_name = (
        package_data.creator.lower().replace(" ", "-").replace(",", "-")
    )

    # Add the double hyphen delimiter.
    dest_file_name += "--"

    # The second part is the title.
    # Adds new words to the filename from the title, but stop after 40
    # characters.
    ascii_letters_digits = string.ascii_letters + string.digits
    title_words = []
    title_words_len = 0
    for title_word in package_data.title.split():
        title_word_filtered = "".join(
            filter(lambda x: x in ascii_letters_digits, title_word)
        )
        if len(dest_file_name) + title_words_len > 120:
            break
        else:
            title_words.append(title_word_filtered)
            title_words_len += len(title_word_filtered)

    dest_file_name += "-".join(title_words)
    dest_file_name = dest_file_name.lower()
    dest_file_name += ".pdf"
    dest_path = os.path.join(files_path, dest_file_name)

    shutil.copy2(thesis_file_path, dest_path)
    return dest_file_name




def copy_package_files(package_data, package_path, files_path):

    thesis_file_name = copy_thesis_pdf(package_data, package_path, files_path)
    thesis_file_path = os.path.join(files_path, thesis_file_name)

    supplemental_path = os.path.join(package_path, "data", "supplemental")
    archive_file_name = None
    archive_file_path = None


    if os.path.isdir(supplemental_path):
        archive_file_name = f"{thesis_file_name[:-4]}-supplemental.zip"
        archive_file_path = os.path.join(files_path, archive_file_name)
        shutil.make_archive(archive_file_path[:-4], "zip", supplemental_path)
        package_data.package_files = [thesis_file_name, archive_file_name]
    else:
        package_data.package_files = [thesis_file_name]

    return thesis_file_path, archive_file_path




def process_agreements(content_lines, mappings):
    """Process the agreements metadata file.

    The package's permissions metadata must state that the embargo period has
    passed and that the student has signed the required agreements.

    Return a list of identifiers to signed agreements.
    """

    # The list of identifiers (term ids).
    agreements = []
    embargo_dates = []
    for line in content_lines:
        line = line.strip()
        if line.startswith(("Student ID", "Thesis ID")):
            continue
        elif "Embargo Expiry" in line:
            embargo_dates.append(line)
            continue
        elif any(line.startswith(name) for name in mappings["agreements"]):
            line_split = line.split("||")
            if line_split[0] not in mappings["agreements"]:
                raise MetadataError(f"{line} is invalid")
            agreement = mappings["agreements"][line_split[0]]
            signed = line_split[2] == "Y"
            if agreement["required"] and not signed:
                raise MetadataError(f"{line} is required but not signed")
            if signed:
                agreements.append(agreement["identifier"])
                continue
            else:
                print(f"LAC agreement not signed")
                continue
        raise MetadataError(
            f"{line} was not expected in the permissions document"
        )

    return agreements, embargo_dates


def create_agreements(package_data, item_output_dir, license_path):
    required_agreements = {
        "Carleton University Thesis License Agreement": "license.txt",
        "FIPPA": "fippa_statement.txt",
        "Academic Integrity Statement": "academic_integrity_statement.txt",
    }

    missing = [
        name
        for name in required_agreements
        if name not in package_data.agreements
    ]

    if missing:
        print(
            f"Skipping item: missing required agreement(s): {', '.join(missing)}"
        )
        return False

    # All required agreements are signed, copy their respective files
    for agreement_name, filename in required_agreements.items():
        src = os.path.join(license_path, filename)
        dst = os.path.join(item_output_dir, filename)
        shutil.copyfile(src, dst)

    return True


def build_metadata_payload(package_data, agreements, thesis_file_path, supplemental_path):

    def add_metadata(dc_key, value):
        if not value:
            return
        if isinstance(value, (list, tuple)):
            metadata[f"{dc_key}"] = [{"value": v} for v in value]
        else:
            metadata[f"{dc_key}"] = [{"value": value}]

    def add_checksum(thesis_file_path, supplemental_path=None):

        def calculate_md5(path):
            hash_md5 = hashlib.md5()
            total_bytes = 0
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
                    total_bytes += len(chunk)
            return total_bytes, hash_md5.hexdigest()

        # Compute MD5s first
        result = {"thesis": calculate_md5(thesis_file_path)}
        if supplemental_path:
            result["supplemental"] = calculate_md5(supplemental_path)

        # Build formatted bitstream info in order of package files
        bitstream_info = []
        for name in package_data.package_files:
            if name.endswith(".pdf"):
                key = "thesis"
            else:
                key = "supplemental"
            size, checksum = result[key]
            bitstream_info.append(f"{name}: {size} bytes, checksum: {checksum} (MD5)")

        # Build final provenance string
        prov_field = (
            f"Made available in DSpace on {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} (GMT). "
            f"No. of bitstreams: {len(package_data.package_files)} "
            + " ".join(bitstream_info)
        )

        return prov_field
    
    
    prov_field = add_checksum(thesis_file_path, supplemental_path)


    metadata = {}

    add_metadata("dc.title", package_data.title)
    add_metadata("dc.contributor.author", package_data.creator)
    add_metadata("dc.contributor.other", package_data.contributors)
    add_metadata("dc.date.issued", package_data.date)
    add_metadata("dc.type", package_data.type)
    add_metadata("dc.description.abstract", package_data.description)
    add_metadata("dc.description.provenance", prov_field)
    add_metadata("dc.publisher", package_data.publisher)
    add_metadata("dc.identifier.doi", package_data.doi)
    add_metadata("dc.language.iso", package_data.language)
    add_metadata("dc.rights", package_data.rights_notes)
    add_metadata("dc.subject.lcsh", package_data.subjects)
    if "LAC Non-Exclusive License" in agreements:
        add_metadata("local.hasLACLicence", True)
    add_metadata(
        "thesis.degree.name",
        package_data.degree + " (" + package_data.abbreviation + ")",
    )
    add_metadata("thesis.degree.discipline", package_data.degree_discipline)
    add_metadata("thesis.degree.level", package_data.degree_level)
    
    return {
        "name": package_data.title,
        "metadata": metadata,
        "inArchive": True,
        "discoverable": True,
        "withdrawn": False,
        "type": "item",
    }


def item_creation(session, api_base, collection_id, metadata_payload):
    item_endpoint = f"{api_base}/core/items?owningCollection={collection_id}"
    response = session.safe_request(
        "POST", item_endpoint, json=metadata_payload
    )
    response.raise_for_status()
    item_uuid = response.json()["uuid"]
    item_handle = response.json()["handle"]
    return item_uuid, item_handle


def bundle_creations(session, api_base, item_uuid):

    bundle_endpoint = f"{api_base}/core/items/{item_uuid}/bundles"
    try:
        response = session.safe_request(
            "POST", bundle_endpoint, json={"name": "ORIGINAL"}
        )
        response.raise_for_status()
        og_bundle_id = response.json()["uuid"]

        response = session.safe_request(
            "POST", bundle_endpoint, json={"name": "LICENSE"}
        )
        response.raise_for_status()
        license_bundle_id = response.json()["uuid"]

        return og_bundle_id, license_bundle_id
    except requests.exceptions.RequestException as e:
        print(f"Error creating bundles: {e}")
        return None, None


def upload_licenses(session, api_base, license_bundle_uuid, license_dir):
    license_files = [
        ("license.txt", "Carleton University License"),
        ("fippa_statement.txt", "FIPPA Agreement"),
        ("academic_integrity_statement.txt", "Academic Integrity Statement"),
    ]

    license_endpoint = (
        f"{api_base}/core/bundles/{license_bundle_uuid}/bitstreams"
    )
    format_id = 2
    format_url = f"{api_base}/core/bitstreamformats/{format_id}"
    headers = {"Content-Type": "text/uri-list"}

    for filename, description in license_files:
        full_path = os.path.join(license_dir, filename)
        if os.path.isfile(full_path):
            with open(full_path, "rb") as file:
                try:
                    license_upload = {"file": (filename, file, "text/plain")}
                    response = session.safe_request(
                        "POST",
                        license_endpoint,
                        files=license_upload,
                        data=LICENSE_BITSTREAM_PAYLOAD,
                    )

                    if response:
                        license_uuid = response.json()["id"]
                        bitstream_endpoint = (
                            f"{api_base}/core/bitstreams/{license_uuid}/format"
                        )

                        try:
                            response = session.safe_request(
                                "PUT",
                                bitstream_endpoint,
                                headers=headers,
                                data=format_url,
                            )
                            response.raise_for_status()
                        except requests.exceptions.RequestException as e:
                            print(
                                f"[{description}] Failed to update MIME type: {e}"
                            )
                except Exception as e:
                    print(f"[{description}] Failed to upload license: {e}")
        else:
            print(f"[{description}] File not found: {full_path}")


def upload_files(
    session,
    api_base,
    package_data,
    og_bundle_uuid,
    file_path,
    metadata_payload,
):

    original_endpoint = f"{api_base}/core/bundles/{og_bundle_uuid}/bitstreams"

    for file_name in package_data.package_files:
        full_path = os.path.join(file_path, file_name)

        if not os.path.isfile(full_path):
            print(f"File not found: {full_path}, skipping")
            continue

        mime_type = mimetypes.guess_type(file_name)[0]

        with open(full_path, "rb") as file:

            if os.path.getsize(full_path) > 1048576000:

                multipart_data = {
                    "file": (file_name, file),
                    "OG_BITSTREAM_PAYLOAD": (
                        None,
                        json.dumps(metadata_payload),
                        "application/json",
                    ),
                }

                e = encoder.MultipartEncoder(multipart_data)
                m = encoder.MultipartEncoderMonitor(
                    e, lambda a: print(a.bytes_read, end="\r")
                )

                def gen():
                    a = m.read(16384)
                    while a:
                        yield a
                        a = m.read(16384)

                try:

                    response = session.safe_request(
                        "POST",
                        original_endpoint,
                        data=gen(),
                        headers={"Content-Type": m.content_type},
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Error with multipart upload of {file_path}: {e}")
                    continue

            else:
                files = {"file": (file_name, file, mime_type)}
                try:
                    response = session.safe_request(
                        "POST",
                        original_endpoint,
                        files=files,
                        data=metadata_payload,
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Error uploading {file_path}: {e}")
                    continue
            print(f"Successfully uploaded: {file_path}")


def create_dspace_import(
    api_base,
    packages,
    invalid_ok,
    doi_start,
    mappings,
    files_path,
    parent_collection_id,
    user_email,
    user_password,
    license_path,
    skipped_path,
    skipped_ids,
    dspace_base_url,
):

    session = DSpaceSession(api_base)
    session.authenticate(user_email, user_password)

    dspace_import_packages = []
    skipped_import_packages = []

    dspace_item_info = {}
    # A list of packages which failed during processing.
    failure_log: List[str] = []
    skipped_log: List[str] = []

    # Start the doi_ident counter at the provided doi_start number.
    doi_ident = doi_start

    click.echo(f"Processing {len(packages)} packages to create Dspace import.")
    for index, package_path in enumerate(packages):
        student_id = os.path.basename(package_path)

        # Is the BagIt container valid? This will catch bit-rot errors early.
        if not bagit.Bag(package_path).is_valid() and not invalid_ok:
            err_msg = "Invalid BagIt."
            click.echo(err_msg)
            failure_log.append(f"{student_id}: {err_msg}")
            continue
        try:

            permissions_path = os.path.join(
                package_path,
                "data",
                "meta",
                f"{student_id}_permissions_meta.txt",
            )
            with open(
                permissions_path, "r", encoding="utf-8"
            ) as permissions_file:
                permissions_file_content = permissions_file.readlines()

            # Note we track all agreements but the only one that matters for metadata purposes is LAC the rest get auto applied if this returns
            agreements, embargo_info = process_agreements(
                permissions_file_content, mappings
            )

            package_metadata_xml_path = os.path.join(
                package_path, "data", "meta", f"{student_id}_etdms_meta.xml"
            )

            package_metadata_xml = ElementTree.parse(package_metadata_xml_path)

            package_data = create_package_data(
                package_metadata_xml,
                student_id,
                doi_ident,
                agreements,
                embargo_info,
                mappings,
            )
            thesis_file_path, supplemental_path = copy_package_files(
                package_data, package_path, files_path
            )

            built_item_payload = build_metadata_payload(
                package_data, agreements, thesis_file_path, supplemental_path
            )

            if student_id in skipped_ids:
                skipped_log.append(
                    f"{student_id}: Skipped (manual processing)"
                )
                click.echo(f"{student_id}: Skipped (manual processing)")

                dest_path = os.path.join(skipped_path, student_id)

                try:
                    skipped_import_packages.append(package_data)
                    shutil.move(package_path, dest_path)
                except shutil.Error as e:
                    click.echo(
                        f"Error moving package {package_path} to skipped directory: {e}"
                    )

                continue
            click.echo(f"{student_id}: ", nl=False)

            item_id, item_handle = item_creation(
                session, api_base, parent_collection_id, built_item_payload
            )

            provenance_delete(session, item_id)
            og_bundle_uuid, license_bundle_uuid = bundle_creations(
                session, api_base, item_id
            )
            upload_licenses(
                session, api_base, license_bundle_uuid, license_path
            )
            upload_files(
                session,
                api_base,
                package_data,
                og_bundle_uuid,
                files_path,
                OG_BITSTREAM_PAYLOAD,
            )

        except ElementTree.ParseError as e:
            err_msg = f"Error parsing XML, {e}."
            click.echo(err_msg)
            failure_log.append(err_msg)
        except MissingFileError as e:
            err_msg = f"Required file is missing, {e}."
            click.echo(err_msg)
            failure_log.append(err_msg)
        except MetadataError as e:
            err_msg = f"Metadata error, {e}."
            click.echo(err_msg)
            failure_log.append(err_msg)
        except FileNotFoundError as e:
            err_msg = f"File Not Found, {e}."
            click.echo(err_msg)
            failure_log.append(err_msg)
        else:
            doi_ident += 1
            if "carleton-dev.scholaris.ca" in dspace_base_url:
                package_data.handle = f"{dspace_base_url}/handle/{item_handle}"
            else:
                package_data.handle = (
                    f"https://hdl.handle.net/20.500.14718/{item_handle}"
                )

            package_data.url = f"{dspace_base_url}/items/{item_id}"
            dspace_import_packages.append(package_data)

    return (
        dspace_import_packages,
        dspace_item_info,
        failure_log,
        skipped_log,
        skipped_import_packages,
    )

def provenance_delete(session, item_id):
    
    endpoint = session.api_base
    
    item_url = f"{endpoint}/core/items/{item_id}"
    response = session.safe_request("GET", item_url)
    response.raise_for_status()
    results = response.json()

    provenance_list = results["metadata"].get("dc.description.provenance", [])
    print(f"Found {len(provenance_list)} provenance entries.")

    updated_list = [
        entry for entry in provenance_list
        if "No. of bitstreams: 0" not in entry["value"]
    ]

    if len(updated_list) == len(provenance_list):
        print("No matching provenance entry found.")
        return

    patch_payload = [
        {
            "op": "replace",
            "path": "/metadata/dc.description.provenance",
            "value": updated_list
        }
    ]

    patch_data = json.dumps(patch_payload)
    resp = session.safe_request(
        "PATCH",
        item_url,
        data=patch_data,
        headers={"Content-Type": "application/json"},
    )

    resp.raise_for_status()
    print(f"Successfully deleted provenance entry from item {item_id}.")
    
def create_crossref_etree():
    doi_batch = ElementTree.Element(
        "doi_batch",
        attrib={
            "version": "4.4.1",
            "xmlns": "http://www.crossref.org/schema/4.4.1",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": (
                "http://www.crossref.org/schema/4.4.1 "
                "http://www.crossref.org/schemas/crossref4.4.1.xsd"
            ),
        },
    )

    head = ElementTree.SubElement(doi_batch, "head")
    doi_batch_id = ElementTree.SubElement(head, "doi_batch_id")
    doi_batch_id.text = str(int(time.time()))
    timestamp = ElementTree.SubElement(head, "timestamp")
    timestamp.text = f"{time.time()*1e7:.0f}"

    depositor = ElementTree.SubElement(head, "depositor")
    depositor_name = ElementTree.SubElement(depositor, "depositor_name")
    depositor_name.text = "Carleton University Library"
    email_address = ElementTree.SubElement(depositor, "email_address")
    email_address.text = "doi@library.carleton.ca"

    registrant = ElementTree.SubElement(head, "registrant")
    registrant.text = "Carleton University"
    body = ElementTree.SubElement(doi_batch, "body")

    tree = ElementTree.ElementTree(doi_batch)
    return tree, body


def create_marc_record(package_data, marc_path):
    """
    Create a MARC encoded record for an ETD package
    """
    subtitle = ""

    if ":" in package_data.title:
        split_title = package_data.title.split(":", 1)
        processed_title = split_title[0].strip() + " :"
        subtitle = split_title[1].strip()
        if subtitle[-1] != ".":
            subtitle = subtitle + "."
    else:
        processed_title = package_data.title.strip()
        if processed_title[-1] != ".":
            processed_title = processed_title + "."

    title_field = pymarc.Field(
        tag="245",
        indicators=["1", "0"],
        subfields=[
            "a",
            processed_title,
        ],
    )

    if subtitle != "":
        title_field.add_subfield("b", subtitle)

    processed_author = package_data.creator.strip()
    if processed_author[-1] != "-":
        processed_author = processed_author + ","

    today = datetime.date.today()

    record = pymarc.Record(force_utf8=True, leader="     nam a22     4i 4500")
    record.add_field(
        pymarc.Field(
            tag="006",
            data="m     o  d        ",
        )
    )
    record.add_field(
        pymarc.Field(
            tag="007",
            data="cr || ||||||||",
        )
    )

    # pub_year = str(package_data.year).ljust(4)[:4]
    record.add_field(
        pymarc.Field(
            tag="008",
            data="{}s{}    onca||||omb|| 000|0 eng d".format(
                today.strftime("%y%m%d"), package_data.date
            ),
        )
    )
    record.add_field(
        pymarc.Field(
            tag="040",
            indicators=[" ", " "],
            subfields=[
                "a",
                "CaOOCC",
                "b",
                "eng",
                "e",
                "rda",
                "c",
                "CaOOCC",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="100",
            indicators=["1", " "],
            subfields=[
                "a",
                processed_author,
                "e",
                "author.",
            ],
        )
    )
    record.add_field(title_field)
    record.add_field(
        pymarc.Field(
            tag="264",
            indicators=[" ", "1"],
            subfields=["a", "Ottawa,", "c", package_data.date],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="264",
            indicators=[" ", "4"],
            subfields=["c", "\u00A9" + package_data.date],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="300",
            indicators=[" ", " "],
            subfields=["a", "1 online resource :", "b", "illustrations"],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="336",
            indicators=[" ", " "],
            subfields=[
                "a",
                "text",
                "b",
                "txt",
                "2",
                "rdacontent",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="337",
            indicators=[" ", " "],
            subfields=[
                "a",
                "computer",
                "b",
                "c",
                "2",
                "rdamedia",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="338",
            indicators=[" ", " "],
            subfields=[
                "a",
                "online resource",
                "b",
                "cr",
                "2",
                "rdacarrier",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="500",
            indicators=[" ", " "],
            subfields=["a", package_data.description],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="502",
            indicators=[" ", " "],
            subfields=[
                "a",
                "Thesis ("
                + package_data.abbreviation
                + ") - Carleton University, "
                + package_data.date
                + ".",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="504",
            indicators=[" ", " "],
            subfields=["a", "Includes bibliographical references."],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="540",
            indicators=[" ", " "],
            subfields=[
                "a",
                (
                    "Licensed through author open access agreement. "
                    "Commercial use prohibited without author's consent."
                ),
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="591",
            indicators=[" ", " "],
            subfields=["a", "e-thesis deposit", "9", "LOCAL"],
        )
    )
    for subject_tags in package_data.deduped_subjects:
        if isinstance(subject_tags, list) and len(subject_tags) % 2 == 0:
            record.add_field(
                pymarc.Field(
                    tag="650", indicators=[" ", "0"], subfields=subject_tags
                )
            )
        else:
            print(f"Invalid subject_tags: {subject_tags}")
    record.add_field(
        pymarc.Field(
            tag="710",
            indicators=["2", " "],
            subfields=[
                "a",
                "Carleton University.",
                "k",
                "Theses and Dissertations.",
                "g",
                package_data.degree_discipline + ".",
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="856",
            indicators=["4", "0"],
            subfields=[
                "u",
                f"{DOI_URL_PREFIX}{package_data.doi}",
                "z",
                (
                    "Free Access "
                    "(Carleton University Institutional Repository Full Text)"
                ),
            ],
        )
    )
    record.add_field(
        pymarc.Field(
            tag="979",
            indicators=[" ", " "],
            subfields=[
                "a",
                "MARC file generated {} on ETD Depositor".format(
                    today.isoformat()
                ),
                "9",
                "LOCAL",
            ],
        )
    )

    with open(
        os.path.join(marc_path, package_data.student_id + "_marc.mrc"), "wb"
    ) as marc_file:
        marc_file.write(record.as_marc())


def resolve_handle_to_uuid(session, handle):

    handle_url = (
        f"{session.api_base.replace('/server/api', '')}/handle/{handle}"
    )
    response = session.safe_request("GET", handle_url, allow_redirects=True)
    response.raise_for_status()
    if response.status_code == 200:
        url = response.url

        if url:
            return url
        else:
            raise ValueError(
                f"UUID not found in redirect for handle: {handle}"
            )
    else:
        raise ValueError(
            f"Unexpected status code {response.status_code} for handle: {handle}"
        )


def build_uuid_map(mapfile_path, session):
    uuid_map = {}
    with open(mapfile_path, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                item_name, handle = parts
                try:
                    uuid = resolve_handle_to_uuid(session, handle)
                    uuid_map[item_name] = uuid
                except Exception as e:
                    print(f"Failed to resolve {handle}: {e}")
    return uuid_map


def post_import_processing(
    session,
    user_email,
    user_password,
    dspace_import_packages,
    dspace_item_info,
    marc_path,
):

    session.authenticate(user_email, user_password)

    # Package data for packages which have been successfully imported
    # into Dspace.
    completed_packages = []

    # Create the ElementTree and body element which will be used to create the
    # Crossref XML.
    crossref_et, body_element = create_crossref_etree()

    # A list of packages which failed during processing.
    failure_log: List[str] = []

    click.echo(
        f"Post-import processing for {len(dspace_import_packages)} packages."
    )

    for package_data in dspace_import_packages:
        click.echo(f"{package_data.title}: ")
        try:
            create_marc_record(package_data, marc_path)
            body_element.append(create_dissertation_element(package_data))
        except GetURLFailedError:
            err_msg = "Link not found in Dspace."
            click.echo(err_msg)
            failure_log.append(f"{package_data.student_id}: {err_msg}")
        except pymarc.exceptions.PymarcException as e:
            err_msg = f"MARC error {e}"
            click.echo(err_msg)
            failure_log.append(f"{package_data.student_id}: {err_msg}")
        else:
            completed_packages.append(package_data)
            click.echo("Done")

    return completed_packages, crossref_et, failure_log


def create_dissertation_element(package_data):
    dissertation = ElementTree.Element("dissertation")

    person_name = ElementTree.SubElement(
        dissertation,
        "person_name",
        attrib={"contributor_role": "author", "sequence": "first"},
    )
    # Unfortunately, Crossref still expects first and last name.
    split_name = package_data.creator.split(",")
    surname_text = split_name[0].strip()
    if len(split_name) == 2:
        given_name_text = split_name[1].strip()
    else:
        given_name_text = ""
    given_name = ElementTree.SubElement(person_name, "given_name")
    given_name.text = given_name_text
    surname = ElementTree.SubElement(person_name, "surname")
    surname.text = surname_text

    titles = ElementTree.SubElement(dissertation, "titles")
    title = ElementTree.SubElement(titles, "title")
    title.text = package_data.title

    approval_date = ElementTree.SubElement(
        dissertation, "approval_date", attrib={"media_type": "online"}
    )
    year = ElementTree.SubElement(approval_date, "year")
    year.text = package_data.date

    institution = ElementTree.SubElement(dissertation, "institution")
    institution_name = ElementTree.SubElement(institution, "institution_name")
    institution_name.text = "Carleton University"
    institution_place = ElementTree.SubElement(
        institution, "institution_place"
    )
    institution_place.text = "Ottawa, Ontario"

    degree = ElementTree.SubElement(dissertation, "degree")
    degree.text = package_data.degree

    doi_data = ElementTree.SubElement(dissertation, "doi_data")
    doi = ElementTree.SubElement(doi_data, "doi")
    doi.text = package_data.doi
    resource = ElementTree.SubElement(doi_data, "resource")
    resource.text = package_data.handle

    return dissertation


def create_csv_list(package_data, csv_file_path):

    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                "Author Name",
                "Package File Name",
                "Date Processed",
                "Link to Thesis in DSpace",
                "PDF File",
                "Supplemental File",
                "Flagged Content",
                "Embargo Files",
            ]
        )

        for data in package_data:
            contents = ""
            author_name = data.creator
            abbreviation = data.degree
            discipline = data.degree_discipline
            package_file_name = data.student_id
            abstract = data.description
            creator = data.creator
            title = data.title
            contributors = data.contributors
            date_processed = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            degree = data.degree
            if degree is FLAG:
                contents += " Degree is flagged."
            if abbreviation is FLAG:
                contents += " Degree abbreviation is flagged."
            if discipline is FLAG:
                contents += " Degree discipline is flagged."
            if "$" in abstract:
                contents += " Abstract contains '$', LaTeX codes?"
            if "\\" in abstract:
                contents += " Abstract contains '\\', LaTeX codes?"
            if "\uFFFD" in title:
                contents += " Title contains replacement character."
            if "\uFFFD" in creator:
                contents += " Creator contains replacement character."
            if "\uFFFD" in abstract:
                contents += " Abstract contains replacement character."
            if "\uFFFD" in str(contributors):
                contents += " Contributors contains replacement character."

            link_to_thesis = data.url
            package_files = data.package_files
            pdf_files = ""
            zip_files = ""
            if len(package_files) > 0:
                pdf_files = ", ".join(
                    [file for file in package_files if file.endswith(".pdf")]
                )
                zip_files = ", ".join(
                    [file for file in package_files if file.endswith(".zip")]
                )

            embargo_info = data.embargo_info
            embargo_info_str = ", ".join(embargo_info) if embargo_info else ""

            writer.writerow(
                [
                    author_name,
                    package_file_name,
                    date_processed,
                    link_to_thesis,
                    pdf_files,
                    zip_files,
                    contents,
                    embargo_info_str,
                ]
            )

    click.echo("Ingest list created successfully.")


def create_postback_files(
    completed_packages, outbox, postback_path, post_import_failure_log
):

    click.echo("Writing postback files: ", nl=False)
    for package in completed_packages:
        try:
            outbox_file = os.path.join(
                outbox, package.student_id + "_postback.txt"
            )
            with open(outbox_file, "w") as postback:
                time_now = (
                    datetime.datetime.now()
                    .replace(second=0, microsecond=0)
                    .isoformat()
                )
                postback.write(
                    "{}||{}||1||{}".format(
                        package.student_id, time_now, package.url
                    )
                )
            continue
        except Exception as e:
            err_msg = (
                f"Warning: Could not write to outbox path ({outbox}): {e}"
            )
            click.echo(err_msg)

        # Fallback to default postback path
        try:
            with open(
                os.path.join(
                    postback_path, package.student_id + "_postback.txt"
                ),
                "w",
            ) as postback:
                time_now = (
                    datetime.datetime.now()
                    .replace(second=0, microsecond=0)
                    .isoformat()
                )
                postback.write(
                    "{}||{}||1||{}".format(
                        package.student_id, time_now, package.url
                    )
                )
        except Exception as e:
            err_msg = f"Error: Failed to write postback file for {package.student_id} to both locations. {e}"
            click.echo(err_msg)
            post_import_failure_log.append(err_msg)
    click.echo("Done")


def send_email_report(
    completed_packages,
    failure_log,
    marc_archive_path,
    crossref_file_path,
    csv_file_path,
    smtp_host,
    smtp_port,
    email_from,
    email_to,
):
    """Send the email report of completed and failed packages.

    This function also attaches the MARC archive and Crossref file."""

    from email.message import EmailMessage

    msg = EmailMessage()

    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = (
        f"ETD Depositor Report - {len(completed_packages)} processed, "
        f"{len(failure_log)} failed"
    )

    contents = (
        "ETD Depository Report - Run on "
        f"{datetime.date.today().isoformat()}.\n\n"
    )
    contents += f"{len(completed_packages)} completed packages.\n"
    for package_data in completed_packages:
        short_title = textwrap.shorten(
            package_data.title, 40, placeholder="..."
        )
        contents += (
            f"{package_data.creator} - {short_title} {package_data.url}"
        )
        contents += "\n"
    contents += "\n"
    contents += f"{len(failure_log)} failed packages.\n"
    for line in failure_log:
        contents += f"{line}\n"

    msg.set_content(contents)

    with open(marc_archive_path, "rb") as marc_archive_file:
        marc_archive_data = marc_archive_file.read()
    msg.add_attachment(
        marc_archive_data,
        maintype="application",
        subtype="zip",
        filename=os.path.basename(marc_archive_path),
    )

    with open(crossref_file_path, "rb") as crossref_file:
        crossref_file_data = crossref_file.read()
    msg.add_attachment(
        crossref_file_data,
        maintype="application",
        subtype="xml",
        filename=os.path.basename(crossref_file_path),
    )
    with open(csv_file_path, "rb") as ingest_list_file:

        ingest_list_data = ingest_list_file.read()
    msg.add_attachment(
        ingest_list_data,
        maintype="application",
        subtype="csv",
        filename=os.path.basename(csv_file_path),
    )

    server = smtplib.SMTP(smtp_host, smtp_port)
    server.send_message(msg)
    server.quit()


@click.command()
@click.argument("base_directory")
@click.option(
    "--api-base",
    default="https://carleton-dev.scholaris.ca/server/api",
    help="Base URL for the DSpace API.",
)
@click.option(
    "--skipped-mappings",
    default="etddepositor/skipped_mappings.yaml",
    help="Path to the skipped mappings YAML file.",
)
@click.option("--outbox", default="", help="Path to the out report directory.")
@click.option(
    "--mapping-file", default="", help="Path to the mappings YAML file."
)
@click.option(
    "--invalid-ok",
    is_flag=True,
    help="Continue processing even if BagIt is invalid.",
)
@click.option(
    "--email-from",
    required=True,
    help="Email address to send the report from.",
)
@click.option(
    "--email-to",
    required=True,
    default="smtp-server.carleton.ca",
    help="Email address to send the report to.",
)
@click.option(
    "--smtp-host", required=True, help="SMTP host for sending emails."
)
@click.option("--smtp-port", type=int, default=25, required=True)
@click.option(
    "--dspace-base-url",
    default="https://carleton-dev.scholaris.ca",
    help="Base URL for DSpace.",
)
@click.option(
    "--doi-start",
    type=int,
    default=1,
    required=True,
    help="The starting number of the incrementing part of the generated DOIs.",
)
@click.option("--user-email", required=True, help=("The DSpace user email."))
@click.option(
    "--user-password",
    prompt=True,
    hide_input=True,
    confirmation_prompt=True,
    help=("The DSpace user password."),
)
@click.option(
    "--parent-collection-id",
    required=True,
    help="The source ID for the parent collection in Dspace.",
)
def process(
    base_directory,
    api_base,
    skipped_mappings,
    outbox,
    doi_start,
    invalid_ok,
    mapping_file,
    user_email,
    user_password,
    email_from,
    email_to,
    smtp_host,
    smtp_port,
    parent_collection_id,
    dspace_base_url,
):

    click.echo("Starting ETD processing...")

    processing_directory = base_directory

    API_BASE = api_base

    # Load mappings
    mappings = load_mappings(mapping_file)
    skipped_mappings = load_mappings(skipped_mappings)

    if not mappings:
        return

    if not skipped_mappings:
        return
    skip_ids = skipped_mappings.get("skip_ids", [])

    # Validate subject mappings
    validate_subject_mappings(mappings)
    (
        done_path,
        marc_path,
        crossref_path,
        csv_report_path,
        file_path,
        failed_path,
        skipped_path,
        license_path,
        postback_path,
    ) = create_output_directories(processing_directory)

    # Find packages in the ready directory
    packages = find_etd_packages(processing_directory)
    click.echo(f"Found {len(packages)} packages to process.")

    if not packages:
        click.echo("No packages found. Exiting.")
        return

    (
        dspace_import_packages,
        dspace_item_info,
        pre_import_failure_log,
        pre_import_skipped_log,
        skipped_import_packages,
    ) = create_dspace_import(
        api_base,
        packages,
        invalid_ok,
        doi_start,
        mappings,
        file_path,
        parent_collection_id,
        user_email,
        user_password,
        license_path,
        skipped_path,
        skip_ids,
        dspace_base_url,
    )

    click.echo("ETD processing complete.")

    click.echo("Starting post import processing")

    session = DSpaceSession(API_BASE)

    (
        completed_packages,
        crossref_et,
        post_import_failure_log,
    ) = post_import_processing(
        session,
        user_email,
        user_password,
        dspace_import_packages,
        dspace_item_info,
        marc_path,
    )

    click.echo("Writing complete CSV file: ", nl=False)
    csv_file_path = os.path.join(
        csv_report_path,
        f"{ datetime.date.today().isoformat()}-ingest_list.csv",
    )
    create_csv_list(completed_packages, csv_file_path)

    click.echo("Writing complete Crossref file: ", nl=False)
    crossref_file_path = os.path.join(
        crossref_path, f"{ datetime.date.today().isoformat()}-crossref.xml"
    )
    crossref_et.write(
        crossref_file_path, encoding="utf-8", xml_declaration=True
    )
    click.echo("Done")

    # TODO: Bug here marc zip attempts to zip itself up point it at another dir or fix it

    click.echo("Creating MARC archive: ", nl=False)
    marc_src_path = os.path.join(processing_directory, MARC_SUBDIR)
    marc_archive_path = os.path.join(
        processing_directory,
        DONE_SUBDIR,
        f"{ datetime.date.today().isoformat()}-marc-archive.zip",
    )
    # make_archive doesn't want the archive extension.
    shutil.make_archive(marc_archive_path[:-4], "zip", marc_src_path)
    click.echo("Done")

    click.echo("Writing postback files: ", nl=False)
    create_postback_files(
        completed_packages, outbox, postback_path, post_import_failure_log
    )

    # Skipped import packages are those that were moved to the skipped directory
    create_postback_files(
        skipped_import_packages, outbox, postback_path, post_import_failure_log
    )

    click.echo("Sending report email: ", nl=False)

    send_email_report(
        completed_packages,
        pre_import_failure_log
        + post_import_failure_log
        + pre_import_skipped_log,
        marc_archive_path,
        crossref_file_path,
        csv_file_path,
        smtp_host,
        smtp_port,
        email_from,
        email_to,
    )
    click.echo("Done")


if __name__ == "__main__":
    process()
