import bagit
import click
import collections
import csv
import datetime
import glob
import grp
import os
import os.path
import pwd
import shutil
import subprocess
import warnings
import xml.etree.ElementTree as ET
import yaml
import zipfile

# AWAITING_WORK_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are copied to before they are processed.
AWAITING_WORK_SUBDIR = "awaiting_work"

# IN_PROGRESS_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are copied when they are being worked on.
IN_PROGRESS_SUBDIR = "in_progress"

# COMPLETE_SUBDIR defines the name of the subdirectory under the processing location
# where etd packages are copied after they have been processed.
COMPLETE_SUBDIR = "complete"

# NAMESPACES is a dictionary of namespace prefixes to URIs.
NAMESPACES = {"dc": "http://purl.org/dc/elements/1.1/"}

# CONTEXT_SETTINGS is a click-specific config dict which allows us to define a
# prefix for the automatic environment variable option feature.
CONTEXT_SETTINGS = {"auto_envvar_prefix": "ETD_DEPOSITOR"}

ETDPackageData = collections.namedtuple(
    "ETDPackageData",
    [
        "title",
        "creator",
        "subject",
        "description",
        "publisher",
        "contributor",
        "date",
        "year",
        "language",
    ],
)


class PermissionsInvalid(Exception):
    pass


class StillInEmbargo(PermissionsInvalid):
    pass


class RequiredAgreementNotSigned(PermissionsInvalid):
    pass


class UnexpectedLine(PermissionsInvalid):
    pass


class MetadataInvalid(Exception):
    pass


class MissingElementTag(MetadataInvalid):
    pass


@click.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.option(
    "--processing-directory",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
)
def etddepositor(ctx, processing_directory):
    """
    Carleton University Library - ETD Deposit Processing Tool
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    ctx.obj["processing_directory"] = processing_directory


@etddepositor.command()
@click.pass_context
@click.option(
    "--inbox",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
)
@click.option("--filetype", type=str, default="zip")
def copy(ctx, inbox, filetype):
    """
    Copy and extract ETD packages from the ITS directory to our local directory.
    """

    # Copy the processing location out of the context object.
    processing_directory = ctx.obj["processing_directory"]

    # Build the awaiting work location path.
    awaiting_work_path = os.path.join(processing_directory, AWAITING_WORK_SUBDIR)

    # Does that path exist?
    if not os.path.isdir(awaiting_work_path):
        click.echo(f"{awaiting_work_path} does not exist yet. Creating now...")
        os.mkdir(awaiting_work_path, mode=0o770)

    # Find the packages that are already in the processing location.
    existing_packages = [
        os.path.basename(x)
        for x in glob.glob(os.path.join(processing_directory, "*", "*"))
    ]

    # Find the list of bags in the ITS directory which aren't already processed.
    new_package_paths = [
        filepath
        for filepath in glob.glob(os.path.join(inbox, f"*.{filetype}"))
        if os.path.splitext(os.path.basename(filepath))[0] not in existing_packages
    ]

    # Extract the files from the ITS directory to awaiting work
    for filepath in new_package_paths:
        click.echo(
            f"Moving bag and extracting {os.path.basename(filepath)}...", nl=False
        )
        try:
            with zipfile.ZipFile(filepath, "r") as packagezip:
                packagezip.extractall(awaiting_work_path)
        except zipfile.BadZipFile as e:
            click.echo(f"{filepath} is a bad zip: {e}")
        except Exception as e:
            print(f"Unable to extract {filepath}: {e}")
        click.echo(f"Done")

    # TODO: Cleanup file permissions after extract.


@etddepositor.command()
@click.pass_context
@click.option(
    "--importer",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    required=True,
)
@click.option("--invalid-ok/--invalid-not-ok", default=False)
def process(ctx, importer, invalid_ok=False):
    """
    Find the oldest unprocessed ETD package and process it.
    """

    # Copy the processing location out of the context object.
    processing_directory = ctx.obj["processing_directory"]

    # Build the three processing location subdirectory paths.
    awaiting_work_path = os.path.join(processing_directory, AWAITING_WORK_SUBDIR)
    in_progress_path = os.path.join(processing_directory, IN_PROGRESS_SUBDIR)
    complete_path = os.path.join(processing_directory, COMPLETE_SUBDIR)

    # Do the 'in progress' and 'complete' directories exist?
    if not os.path.isdir(in_progress_path):
        click.echo(f"{in_progress_path} does not exist yet. Creating now...")
        os.mkdir(in_progress_path, mode=0o770)
    if not os.path.isdir(complete_path):
        click.echo(f"{complete_path} does not exist yet. Creating now...")
        os.mkdir(complete_path, mode=0o770)

    # Get a list of the package directories that are in the awaiting work directory.
    packages_awaiting_work = glob.glob(os.path.join(awaiting_work_path, "*"))

    # Use the old 'less than inf' trick to find the oldest package.
    oldest_etd_package_path = None
    oldest_etd_package_mtime = float("inf")

    for package_path in packages_awaiting_work:
        modified_time = os.path.getmtime(package_path)
        if modified_time < oldest_etd_package_mtime:
            oldest_etd_package_mtime = modified_time
            oldest_etd_package_path = package_path

    if (
        oldest_etd_package_path
        and not invalid_ok
        and not bagit.Bag(oldest_etd_package_path).is_valid()
    ):
        click.echo(
            f"Unable to process {os.path.basename(oldest_etd_package_path)}, BagIt file is not valid."
        )
        oldest_etd_package_path = None

    if oldest_etd_package_path is None:
        click.echo("No valid packages found to process.")
        raise click.Abort

    click.echo(
        f"Moving {os.path.basename(oldest_etd_package_path)} to 'in progress' directory."
    )

    shutil.move(oldest_etd_package_path, in_progress_path)

    in_progress_package_path = os.path.join(
        in_progress_path, os.path.basename(oldest_etd_package_path)
    )
    package_basename = os.path.basename(in_progress_package_path)
    permissions_metadata_path = os.path.join(
        in_progress_package_path,
        "data",
        "meta",
        f"{package_basename}_permissions_meta.txt",
    )

    with open(permissions_metadata_path, "r") as permissions_document:
        validate_permissions_document(permissions_document.read())

    package_metadata_xml_path = os.path.join(
        in_progress_package_path, "data", "meta", f"{package_basename}_etdms_meta.xml"
    )
    tree = ET.parse(package_metadata_xml_path)
    package_data = extract_metadata(tree.getroot())

    # click.echo(package_data)

    # TODO: Move to completed and clean up file ownership.
    csv_path = csv_exporter(package_data, in_progress_package_path)

    new_bagit = str(datetime.date.today()) + "-" + package_basename
    new_bagit_directory = os.path.join(in_progress_path, new_bagit)

    # Make new directory for bagit to be created and input all data information
    shutil.copytree(in_progress_package_path + "/data", new_bagit_directory)

    # Remove metadata folder and LAC agreement (TODO: Check after for hiding certain files)
    shutil.rmtree(os.path.join(new_bagit_directory, "meta"))
    shutil.rmtree(os.path.join(new_bagit_directory, "LAC"))

    if os.path.isdir("contributor"):
        shutil.rmtree(os.path.join(new_bagit_directory, "contributor"))

    bagit.make_bag(new_bagit_directory, {"Contact-Name": package_data.creator})
    shutil.copyfile(
        in_progress_package_path + "/metadata.csv",
        new_bagit_directory + "/metadata.csv",
    )

    click.echo(
        f"Deleting old {os.path.basename(in_progress_package_path)} from 'in progress' directory."
    )
    shutil.rmtree(in_progress_package_path)
    click.echo(
        f"Moving new bagit {os.path.basename(new_bagit_directory)} to 'complete' directory."
    )

    complete_path_bagit = shutil.move(new_bagit_directory, complete_path)
    shutil.make_archive(complete_path_bagit, "zip", complete_path_bagit)

    # import_bagit(importer, complete_path_bagit)
    click.echo(f"Package process complete!")

    click.echo(f"Importing bagit...")
    subprocess.run(
        [
            importer,
            "--name",
            os.path.basename(complete_path_bagit),
            "--parser_klass",
            "Bulkrax::BagitParser",
            "--metadata_file_name",
            "metadata.csv",
            "--metadata_format",
            "Bulkrax::CsvEntry",
            "--commit",
            "Create and Import",
            "--import_file_path",
            complete_path_bagit,
            "--user_id",
            "1",
            "--auth_token",
            "12345",
        ]
    )


def validate_permissions_document(content):
    for line in content.strip().split("\n"):
        if line.startswith("Student ID"):
            continue
        elif line.startswith("Thesis ID"):
            continue
        elif line.startswith("Embargo Expiry"):
            current_date = datetime.date.today()
            expiry_date = line.split(" ")[2]
            embargo_date = embargo_string_to_datetime(expiry_date)
            if current_date < embargo_date:
                raise StillInEmbargo(
                    f"The embargo date of {expiry_date} has not passed."
                )
        elif line.startswith("LAC Non-Exclusive License"):
            continue
        elif line.startswith(
            (
                "Academic Integrity Statement",
                "FIPPA",
                "Carleton University Thesis License Agreement",
            )
        ):
            if line.split("||")[2] != "Y":
                raise RequiredAgreementNotSigned(f"{line} is invalid.")
        else:
            raise UnexpectedLine(
                f"{line} was not expected in the permissions document content."
            )


def embargo_string_to_datetime(embargo):
    month_to_int = {
        "JAN": "1",
        "FEB": "2",
        "MAR": "3",
        "APR": "4",
        "MAY": "5",
        "JUN": "6",
        "JUL": "7",
        "AUG": "8",
        "SEP": "9",
        "OCT": "10",
        "NOV": "11",
        "DEC": "12",
    }
    embargo_split = embargo.split("-")
    month_number = month_to_int[embargo_split[1]]
    formatted_date = f"{embargo_split[0]}/{month_number}/{embargo_split[2]}"
    return datetime.datetime.strptime(formatted_date, "%d/%m/%y").date()


def extract_metadata(root):

    title = root.findall("dc:title", namespaces=NAMESPACES)
    creator = root.findall("dc:creator", namespaces=NAMESPACES)

    if not title:
        raise MissingElementTag(f"title element tag was not found")
    else:
        if title[0].text.strip() == "":
            raise MissingElementTag(f"title element tag was found but was empty")
    if not creator:
        raise MissingElementTag(f"creator element tag was not found")
    else:
        if creator[0].text.strip() == "":
            raise MissingElementTag(f"creator element tag was found but was empty")

    title = title[0].text.strip()
    creator = creator[0].text.strip()

    subject = root.findall("dc:subject", namespaces=NAMESPACES)
    description = root.findall("dc:description", namespaces=NAMESPACES)
    publisher = root.findall("dc:publisher", namespaces=NAMESPACES)
    contributor = root.findall("dc:contributor", namespaces=NAMESPACES)
    date = root.findall("dc:date", namespaces=NAMESPACES)
    language = root.findall("dc:language", namespaces=NAMESPACES)

    subject = check_metadata(subject, "subject")
    description = check_metadata(description, "description")
    publisher = check_metadata(publisher, "publisher")

    if contributor:
        contributor = contributor[0].text.strip()

    date = check_metadata(date, "date")
    language = check_metadata(language, "language")

    year = date[:4]

    print(year)
    name = root.find("{http://www.ndltd.org/standards/metadata/etdms/1.1/}name")
    # print("name ======", name)

    data = ETDPackageData(
        title=title,
        creator=creator,
        subject=subject,
        description=description,
        publisher=publisher,
        contributor=contributor,
        date=date,
        year=year,
        language=language,
    )

    return data


def check_metadata(data, xml_tag):

    with open("degree_config.yaml") as config_file:
        config_yaml = yaml.load(config_file, Loader=yaml.FullLoader)

    if data[0].text.strip() == "":
        warnings.warn(f"{xml_tag} element tag was empty")

    if xml_tag == "description":
        data = data[0].text.strip()
        data = data.replace("\n", " ")
        data = data.replace("\r", "")
        data = data.replace("\u2018", "'")
        data = data.replace("\u2019", "'")
        data = data.replace("\u201c", '"')
        data = data.replace("\u201d", '"')
        data = data.replace("\u2013", "-")
        for symbol in config_yaml["html_escape_table"].keys():
            data = data.replace(config_yaml["html_escape_table"][symbol], symbol)

        return data

    elif xml_tag == "subject":
        data = data[0].text.strip()

        if config_yaml["proquest_subject"].get(data):
            return config_yaml["proquest_subject"].get(data)
        else:
            warnings.warn(f"proquest subject code does not map to known value")
            return data

    elif xml_tag == "language":
        if data[0].text.strip() != "eng" and "fre":
            warnings.warn(f"language tag was not an expected eng or fre tag")

    elif xml_tag == "date":
        try:
            date_obj = datetime.datetime.strptime(data[0].text.strip(), "%Y-%M-%d")
        except:
            warnings.warn(f"date tag is not in expected YYYY-MM-DD format")

    data = data[0].text.strip()
    return data


def csv_exporter(data, path):
    """Creates csv with metadata information"""

    columns = [
        "source_identifier",
        "title",
        "creator",
        "subject",
        "description",
        "publisher",
        "contributor",
        "date",
        "year",
        "language",
    ]
    rows = []

    rows.append(os.path.basename(path))
    rows.append(data.title)
    rows.append(data.creator)
    rows.append(data.subject)
    rows.append(data.description)
    rows.append(data.publisher)
    rows.append(data.contributor)
    rows.append(data.date)
    rows.append(data.year)
    rows.append(data.language)

    with open(path + "/metadata.csv", "w", newline="") as csvfile:
        metadatawriter = csv.writer(csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        metadatawriter.writerow(columns)
        metadatawriter.writerow(rows)
    click.echo(f"Created csv metadata for {os.path.basename(path)}")

    return path + "metadata.csv"


@etddepositor.command()
@click.pass_context
# @click.option("--invalid-ok/--invalid-not-ok", default=False)
def change(path):
    uid = os.getuid()
    gid = os.getgid()
    print(f"{uid} and {gid}")
    print(f"{path}")


def import_bagit(importer, bagit_path):

    subprocess.run(
        [
            importer,
            "--name",
            os.path.basename(bagit_path),
            "--parser_klass",
            "Bulkrax::BagitParser",
            "--metadata_file_name",
            "metadata.csv",
            "--metadata_format",
            "Bulkrax::CsvEntry",
            "--commit",
            "Create and Import",
            "--import_file_path",
            bagit_path,
            "--user_id",
            "1",
            "--auth_token",
            "12345",
        ]
    )


if __name__ == "__main__":
    etddepositor(obj={})
