import pprint
import time
#import click
import requests
import os
import sqlite3
import hashlib
from urllib.parse import urljoin
import json
import yaml
import logging
import mimetypes
import shutil
import random
import datetime
import logger

# Configuration
SOLR_URL = "http://localhost:8983/solr/blacklight-core"
FEDORA_BASE_URL = "http://localhost:8080/fcrepo/rest/prod/"
DB_PATH = "migration.db"
API_BASE = "https://carleton-dev.scholaris.ca/server/api"
DSPACE_BASE_URL = "https://carleton-dev.scholaris.ca"

# These 4 click options
YAML_PATH = "/home/manfred/hyrax-to-dspace-migrate/mapping_config.yaml"
GEO_PATH = "/home/manfred/hyrax-to-dspace-migrate/geo_name.yaml"
TMP_DIR = "/home/manfred/test_zone/tmp_files/"
user = "manfredraffelsieper@cunet.carleton.ca"
password = "88DD434846F8D7F51041A5A25C843BAD"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

#Add Error file handler
file_handler = logging.FileHandler('my_application.log')
file_handler.setLevel(logging.ERROR) 
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#Add General Log
all_logs_file_handler = logging.FileHandler('all_logs.log')
all_logs_file_handler.setLevel(logging.INFO) 
all_logs_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
all_logs_file_handler.setFormatter(all_logs_formatter)
logger.addHandler(all_logs_file_handler)

os.makedirs(TMP_DIR, exist_ok=True)

og_bundle_metadata_payload = {
    "name": "ORIGINAL"
}

license_bundle_metadata_payload = {
    "name": "LICENSE"
}

og_bitstream_payload = { 
                "name": "", 
                "description": "",
                "type": "bitstream",
                "bundleName": "ORIGINAL" 
                }

license_bitstream_payload = {
                "name": "", 
                "description": "",
                "type": "license",
                "bundleName": "LICENSE" 
                }

def load_mappings(file_path):
    with open(file_path, "r") as f:
        mappings = yaml.safe_load(f)
    return mappings["fedora_to_dspace"]

def get_dspace_collection(fedora_id, mappings):
    return mappings.get(fedora_id, None)


# This class handles the the csrf token handling 
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
            response = self.post(f"{self.api_base}/authn/login", data=login_payload)
            response.raise_for_status()
            self.update_csrf_token(response)

            self.auth_token = response.headers.get("Authorization")
            if self.auth_token:
                self.headers.update({"Authorization": self.auth_token})
                self.last_auth_time = time.time()
                logger.info(f"Authentication successful for user: {user}")
            else:
                logger.warning(f"Authentication response did not contain a token for user: {user}")

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error during authentication for user {user}: {e}")
            logger.debug(f"Response content: {response.text}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error during authentication for user {user}: {e}")

        except Exception as e:
            logger.error(f"Unexpected error during authentication for user {user}: {e}")

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
            logger.info(f"Successful {method} request to {url}")  
            return response

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error during {method} request to {url}: {e}")
            logger.debug(f"Response content: {response.text}")  

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error during {method} request to {url}: {e}")

        except Exception as e:
            logger.error(f"Unexpected error during {method} request to {url}: {e}")

    def safe_request(self, method, url, **kwargs):
        return self.request(method, url, **kwargs)

# Item Creation made within dspace
def item_creation(session, collection_id, metadata_payload):
    try:
        mappings = load_mappings(YAML_PATH)
        if len(collection_id) > 1:
            item_uuid = None  
            mapping = get_dspace_collection(collection_id[0], mappings)
            if mapping:
                dspace_collection_id = mapping["dspace_collection_id"]
                item_endpoint = f"{API_BASE}/core/items?owningCollection={dspace_collection_id}"
                response = session.safe_request("POST", item_endpoint, json=metadata_payload)
                response.raise_for_status()
                item_uuid = response.json()["uuid"]  
            for i in collection_id[1:]:
                mapping = get_dspace_collection(i, mappings)
                if mapping:
                    headers = {"Content-Type": "text/uri-list"}
                    dspace_collection_id = mapping["dspace_collection_id"]
                    collection_uri = f"{API_BASE}/core/collections/{dspace_collection_id}"
                    
                    mapping_endpoint = f"{API_BASE}/core/items/{item_uuid}/mappedCollections"
                    mapping_response = session.safe_request("POST", mapping_endpoint, headers=headers, data=collection_uri)
                    mapping_response.raise_for_status()
            return item_uuid
        else:
            mapping = get_dspace_collection(collection_id[0], mappings)
            if mapping:
                dspace_collection_id = mapping["dspace_collection_id"]
                item_endpoint = f"{API_BASE}/core/items?owningCollection={dspace_collection_id}"
                response = session.safe_request("POST", item_endpoint, json=metadata_payload)
                response.raise_for_status()
                item_uuid = response.json()["uuid"] 
                return item_uuid 
    except requests.exceptions.RequestException as req_err:
        logger.warning(f"Request error occurred: {req_err}")
        try:
            logger.warning(f"Request URL: {response.request.url}") 
            logger.warning(f"Request headers: {response.request.headers}") 
            logger.warning(f"Request body: {response.request.body}") 
        except NameError:
            logger.warning("Response object not defined (RequestException)")
        except Exception as e:
            logger.warning(f"Error accessing request details: {e}")

    except Exception as err:
        logger.error(f"An unexpected error occurred: {err}")

# Bundle Creation, this one specifically focuses will store the PDF or ZIPs of ETD's
def original_bundle_creation(session, item_uuid, og_bundle_payload):
    bundle_endpoint = f"{API_BASE}/core/items/{item_uuid}/bundles"
    try:
        response = session.safe_request("POST", bundle_endpoint, json=og_bundle_payload)
        response.raise_for_status()
        og_bundle_id = response.json()["uuid"]
        return og_bundle_id

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred during bundle creation: {http_err}")
        try:
            logger.error(f"Response content: {response.content.decode('utf-8')}")
            logger.error(f"Response headers: {response.headers}")
            logger.error(f"Request URL: {response.request.url}")
        except NameError:
            logger.error("Response object not defined (HTTPError)")
        except Exception as e:
            logger.error(f"Error decoding response content: {e}")

    except requests.exceptions.RequestException as req_err:
        logger.warning(f"Request error occurred during bundle creation: {req_err}")
        try:
            logger.warning(f"Request URL: {response.request.url}")
            logger.warning(f"Request headers: {response.request.headers}")
            logger.warning(f"Request body: {response.request.body}")
        except NameError:
            logger.warning("Response object not defined (RequestException)")
        except Exception as e:
            logger.warning(f"Error accessing request details: {e}")

    except Exception as err:
        logger.error(f"An unexpected error occurred during bundle creation: {err}")

def licence_bitstream_endpoint(session, license_bundle_uuid, licence_list):
    cu_license_path = "/home/manfred/test_zone/licenses/license.txt"
    license_endpoint = f"{API_BASE}/core/bundles/{license_bundle_uuid}/bitstreams"
    path = "/home/manfred/test_zone/licenses/"
    try:
        if licence_list == False:
            if os.path.isfile(cu_license_path):
                with open(cu_license_path, "rb") as file:
                    try:
                        license = {"file": ("license.txt", file, "text/plain")}
                        response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                        
                        if response:
                            license_uuid = response.json()["id"]
                            license_bitstream_endpoint = f"{API_BASE}/core/bitstreams/{license_uuid}/format"

                            format_id = 2
                            format_url = f"{API_BASE}/core/bitstreamformats/{format_id}"
                            headers = {"Content-Type": "text/uri-list"}
                            try:
                                response = session.safe_request("PUT", license_bitstream_endpoint, headers=headers, data=format_url)
                                response.raise_for_status()
                                logger.info(f"Updated bitstream {license_uuid} to MIME type 'text/uri-list' (format ID {format_id})")
                            except requests.exceptions.RequestException as e:
                                logger.error(f"Error updating MIME type for bitstream {license_uuid}: {e}")

                        else:
                            retries = 0
                            while retries < 5:
                                try:
                                    response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                                    response.raise_for_status()
                                    return response
                                except requests.exceptions.RequestException as e:
                                    logging.error(f"Upload failed (retry {retries + 1}): {e}")
                                    retries += 1
                                    delay = (2 ** retries) + random.random()  # Exponential backoff with jitter
                                    time.sleep(delay)
                            logging.error("Upload failed after multiple retries.")
                            return None
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Error uploading {file_path}: {e}")
        else:
            for lic_name in os.listdir(path):
                if lic_name in licence_list and licence_list[lic_name] and lic_name == "old_cu_license.txt":
                    file_path = os.path.join(path, lic_name)
                    if os.path.isfile(file_path):
                        with open(file_path, "rb") as file:
                            try:        
                                license = {"file": ("license.txt", file, "text/plain")}
                                response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                                if response:
                                    license_uuid = response.json()["id"]
                                    license_bitstream_endpoint = f"{API_BASE}/core/bitstreams/{license_uuid}/format"

                                    format_id = 2
                                    format_url = f"{API_BASE}/core/bitstreamformats/{format_id}"
                                    headers = {"Content-Type": "text/uri-list"}
                                    try:
                                        response = session.safe_request("PUT", license_bitstream_endpoint, headers=headers, data=format_url)
                                        response.raise_for_status()
                                        logger.info(f"Updated bitstream {license_uuid} to MIME type 'text/uri-list' (format ID {format_id})")
                                    except requests.exceptions.RequestException as e:
                                        logger.error(f"Error updating MIME type for bitstream {license_uuid}: {e}")
                                else:
                                    retries =0
                                    while retries < 5:
                                        try:
                                            response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                                            response.raise_for_status()
                                            return response
                                        except requests.exceptions.RequestException as e:
                                            logging.error(f"Upload failed (retry {retries + 1}): {e}")
                                            retries += 1
                                            delay = (2 ** retries) + random.random()  # Exponential backoff with jitter
                                            time.sleep(delay)
                                    logging.error("Upload failed after multiple retries.")
                                    return None

                            except requests.exceptions.RequestException as e:
                                logger.error(f"Error uploading {file_path}: {e}")
                elif lic_name in licence_list and licence_list[lic_name] and lic_name == "deposit_agreement.txt":
                    file_path = os.path.join(path, lic_name)
                    if os.path.isfile(file_path):
                        with open(file_path, "rb") as file:
                            try:        
                                license = {"file": ("license.txt", file, "text/plain")}
                                response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                                if response:
                                    license_uuid = response.json()["id"]
                                    license_bitstream_endpoint = f"{API_BASE}/core/bitstreams/{license_uuid}/format"

                                    format_id = 2
                                    format_url = f"{API_BASE}/core/bitstreamformats/{format_id}"
                                    headers = {"Content-Type": "text/uri-list"}
                                    try:
                                        response = session.safe_request("PUT", license_bitstream_endpoint, headers=headers, data=format_url)
                                        response.raise_for_status()
                                        logger.info(f"Updated bitstream {license_uuid} to MIME type 'text/uri-list' (format ID {format_id})")
                                    except requests.exceptions.RequestException as e:
                                        logger.error(f"Error updating MIME type for bitstream {license_uuid}: {e}")
                                else:
                                    retries = 0 
                                    while retries < 5:
                                        try:
                                            response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                                            response.raise_for_status()
                                            return response
                                        except requests.exceptions.RequestException as e:
                                            logging.error(f"Upload failed (retry {retries + 1}): {e}")
                                            retries += 1
                                            delay = (2 ** retries) + random.random()  # Exponential backoff with jitter
                                            time.sleep(delay)
                                    logging.error("Upload failed after multiple retries.")
                                    return None
                            except requests.exceptions.RequestException as e:
                                logger.error(f"Error uploading {file_path}: {e}")
                elif lic_name in licence_list and licence_list[lic_name]:
                    file_path = os.path.join(path, lic_name)
                    if os.path.isfile(file_path):
                        with open(file_path, "rb") as file:
                            try:        
                                license = {"file": (lic_name, file, "text/plain")}
                                response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                                if response:
                                    license_uuid = response.json()["id"]
                                    license_bitstream_endpoint = f"{API_BASE}/core/bitstreams/{license_uuid}/format"

                                    format_id = 2
                                    format_url = f"{API_BASE}/core/bitstreamformats/{format_id}"
                                    headers = {"Content-Type": "text/uri-list"}
                                    try:
                                        response = session.safe_request("PUT", license_bitstream_endpoint, headers=headers, data=format_url)
                                        response.raise_for_status()
                                        logger.info(f"Updated bitstream {license_uuid} to MIME type 'text/uri-list' (format ID {format_id})")
                                    except requests.exceptions.RequestException as e:
                                        logger.error(f"Error updating MIME type for bitstream {license_uuid}: {e}")
                                else:
                                    retries = 0
                                    while retries < 5:
                                        try:
                                            response = session.safe_request("POST", license_endpoint, files=license, data=license_bitstream_payload)
                                            response.raise_for_status()
                                            return response
                                        except requests.exceptions.RequestException as e:
                                            logging.error(f"Upload failed (retry {retries + 1}): {e}")
                                            retries += 1
                                            delay = (2 ** retries) + random.random()  # Exponential backoff with jitter
                                            time.sleep(delay)
                                    logging.error("Upload failed after multiple retries.")
                                    return None
                            except requests.exceptions.RequestException as e:
                                logger.error(f"Error uploading {file_path}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
# session: passing in csrf token from session
# UUID: Both the item bundle id and the license bundle id needed for endpoint
# bitstream_path: Path directed at downloaded fedora files
# og_bitstream_payload: payload is required for the endpoint its just a simple payload for the bundle
def original_bitstream_endpoint(session, og_bundle_uuid, bitstream_path, og_bitstream_payload):
    original_endpoint = f"{API_BASE}/core/bundles/{og_bundle_uuid}/bitstreams"
    bitstream_metadata = []
    for file_name in os.listdir(bitstream_path):
        mime_type = mimetypes.guess_type(file_name)[0]
        file_path = os.path.join(bitstream_path, file_name)  
        
        if os.path.isfile(file_path):   
            with open(file_path, "rb") as file:

                if os.path.getsize(file_path) > 1048576000:

                    multipart_data = {
                        'file': (file_name, file),
                        'og_bitstream_payload': (None, json.dumps(og_bitstream_payload), 'application/json')
                    }
                
                    e = encoder.MultipartEncoder(multipart_data)
                    m = encoder.MultipartEncoderMonitor(e, lambda a: print(a.bytes_read, end='\r'))

                    def gen():
                        a = m.read(16384)
                        while a:
                            yield a
                            a = m.read(16384)
                    try:

                        response = session.safe_request("POST", original_endpoint, data=gen(), headers={"Content-Type": m.content_type})
                        response.raise_for_status()
                        data = json.loads(response.text)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Error with multipart upload of {file_path}: {e}")
                        continue

                else:
                    files = {"file": (file_name, file, mime_type)}
                    try:
                        response = session.safe_request("POST", original_endpoint, files=files, data=og_bitstream_payload)
                        response.raise_for_status()
                        data = json.loads(response.text)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Error uploading {file_path}: {e}")
                        continue

                    
                bitstream_metadata.append({
                    "bitstream_url": f"{DSPACE_BASE_URL}/bitstreams/{data.get('id')}/download",
                    "bitstream_type": data.get("name"),
                    "bitstream_md5" : data.get("checkSum", {}).get("value"),
                    "bitstream_byte": data.get("sizeBytes")

                })
                logger.info(f"Successfully uploaded: {file_path}") 
        else:
            logger.info(f"Skipping directory: {file_path}")
    return bitstream_metadata
        
def upload_with_retries(session, url, files, data, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            response = session.safe_request("POST", url, files=files, data=data)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Upload failed (retry {retries + 1}): {e}")
            retries += 1
            delay = (2 ** retries) + random.random()  # Exponential backoff with jitter
            time.sleep(delay)
    logging.error("Upload failed after multiple retries.")
    return None

def create_provenance_string(session, bitstream_metadata, item_id):
    bitstream_info_parts = []

    item_metadata_endpoint = f"{API_BASE}/core/items/{item_id}"
    try:
        response = session.safe_request("GET", item_metadata_endpoint)
        response.raise_for_status()
        metadata = response.json()
        
        for entry in metadata.get('metadata', {}).get('dc.date.issued', []):
                date_issued = entry.get("value")[:4]

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving date: {e}")
        
    except ValueError as e:
        print(f"Error decoding json: {e}")
        

    for bitstream_data in bitstream_metadata:
        bitstream_type = bitstream_data.get('bitstream_type')
        bitstream_byte = bitstream_data.get('bitstream_byte')
        bitstream_md5 = bitstream_data.get('bitstream_md5')

        bitstream_info_parts.append(f"{bitstream_type}: {bitstream_byte} bytes, checksum: {bitstream_md5} (MD5)")

    bitstream_info = " ".join(bitstream_info_parts)
    timestamp = datetime.datetime.utcnow().isoformat() + "Z (GMT)"

    
    provenance_string = (
        f"Migrated from Hyrax on {timestamp}. "
        f"No. of bitstreams: {len(bitstream_metadata)} {bitstream_info}. "
        f"Date Issued: {date_issued}."
    )

    return provenance_string

def provenance_update(session, item_id, bitstream_metadata):
    provenance_entry = create_provenance_string(session, bitstream_metadata, item_id)
    item_metadata_endpoint = f"{API_BASE}/core/items/{item_id}"
    patch_data = [
        {
        "op": "replace",
        "path": "/metadata/dc.description.provenance",
        "value": provenance_entry
        }   
    ]
    patch_data = json.dumps(patch_data)
    session.safe_request("PATCH", item_metadata_endpoint, data=patch_data, headers={"Content-Type": "application/json"})

def submission_creation(session, collection_id):
    
    submission_endpoint = f"{API_BASE}/submission/workspaceitems?owningCollection={collection_id}"

    headers = {"Content-Type": "application/json"}
    data = {}

    response = session.safe_request("POST", submission_endpoint, headers=headers, json=data)
    response.raise_for_status()

    print("response")
    pprint.pprint(response.json())
    submission_id = response.json()["id"] 
    return submission_id

def submission_workflow(session, collection_id):

    submission_id = submission_creation(session, collection_id)
    print(submission_id)

def item_workflow(session):
    item_uuid = item_creation(session, member_of, payload)
    og_bundle_uuid = original_bundle_creation(session, item_uuid, og_bundle_metadata_payload)
    license_bundle_uuid = license_bundle_creation (session, item_uuid, license_bundle_metadata_payload)
    licence_bitstream_endpoint(session, license_bundle_uuid, license_list)
    bitstream_metadata = original_bitstream_endpoint(session, og_bundle_uuid, file_path, og_bundle_metadata_payload)
    provenance_update(session, item_uuid, bitstream_metadata)
    
def transfer_dspace(session, user, password):
    
    try:
        session.authenticate(user, password)

        submission_workflow(session, "e0dc7bb9-9a07-454a-befb-42a434591436")
        #item_workflow(session)
        print(submission_workflow)
        
    except requests.exceptions.RequestException as req_err:
        logger.warning(f"Request error occurred: {req_err}")  

    except Exception as err:
        logger.error(f"An unexpected error occurred: {err}")



def process_resource(session):

    dspace_data = transfer_dspace(session, user, password)
    
        

#Priority Top to Bottom 
if __name__ == "__main__":

    
    session = DSpaceSession(API_BASE)
    try:
        process_resource(session)
    except Exception as e:
        print(f"Error processing {e}")

