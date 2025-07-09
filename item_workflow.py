import pprint
import time
#import click
import requests
import os
from urllib.parse import urljoin
import json
import logging
import mimetypes
import shutil
import random
import datetime
from requests_toolbelt.multipart import encoder

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

def add_metadata(session, submission_id):
    metadata_payload = [
        {
            "op": "add",
            "path": "/sections/traditionalpageone-carleton/dc.contributor.author",
            "value": [{
                "value": "Gary",
                "language": None,
                "authority": None,
                "confidence": 500
            }]
        },
        {
            "op": "add",
            "path": "/sections/traditionalpageone-carleton/dc.contributor.other",
            "value": [{
                "value": "Spector",
                "language": None,
                "authority": None,
                "confidence": 500
            }]
        },
        {
            "op": "add",
            "path": "/sections/traditionalpageone-carleton/dc.date.issued",
            "value": [{
                "value": "2025-06-19",
                "language": None,
                "authority": None,
                "confidence": 500
            }]
        },
        {
            "op": "add",
            "path": "/sections/traditionalpageone-carleton/dc.title",
            "value": [{
                "value": "New Test Submission",
                "language": None,
                "authority": None,
                "confidence": 500
            }]
        },
        {
            "op": "add",
            "path": "/sections/traditionalpageone-carleton/dc.identifier.doi",
            "value": [{
                "value": "10.10.10",
                "language": None,
                "authority": None,
                "confidence": 500
            }]
        },
        {
            "op": "replace",
            "path": "/sections/license/granted",
            "value": True
        }
    
    ]
    
    endpoint = f"{API_BASE}/submission/workspaceitems/{submission_id}"

    try:
        response = session.safe_request("PATCH", endpoint, json=metadata_payload)
        response.raise_for_status()
        print("Metadata added successfully:", response.json())
        return response.json()
    except requests.exceptions.HTTPError as e:
        print("Error adding metadata:", e)
        return None

      
def item_creation(session, collection_id, metadata_payload):

    item_endpoint = f"{API_BASE}/core/items?owningCollection={collection_id}"
    response = session.safe_request("POST", item_endpoint, json=metadata_payload)
    response.raise_for_status()
    item_uuid = response.json()["uuid"]  
    return item_uuid

def bundle_creations(session, item_uuid):
    
    bundle_endpoint = f"{API_BASE}/core/items/{item_uuid}/bundles"
    try:
        response = session.safe_request("POST", bundle_endpoint, json={"name":"ORIGINAL"})
        response.raise_for_status()
        og_bundle_id = response.json()["uuid"]
        
        response = session.safe_request("POST", bundle_endpoint, json={"name": "LICENSE"})
        response.raise_for_status()
        license_bundle_id = response.json()["uuid"]
        
        return og_bundle_id, license_bundle_id
    except requests.exceptions.RequestException as e:
        print(f"Error creating bundles: {e}")
        return None, None

def original_bundle_creation(session, item_uuid, og_bundle_payload):
    bundle_endpoint = f"{API_BASE}/core/items/{item_uuid}/bundles"
    try:
        response = session.safe_request("POST", bundle_endpoint, json=og_bundle_payload)
        response.raise_for_status()
        og_bundle_id = response.json()["uuid"]
        return og_bundle_id
    except:
        print("Fix me")

#TODO: Make a method or loop this together so we dont have repeated code
#TODO: Make the paths passed in or static dont just keep add future problems for yourself

def upload_licenses(session, license_bundle_uuid):
    cu_license_path = "/home/manfredraffelsieper/etddepositor_project/processing_dir/license/license.txt"
    fippa_agreement_path = "/home/manfredraffelsieper/etddepositor_project/processing_dir/license/fippa_agreement.txt"
    license_endpoint = f"{API_BASE}/core/bundles/{license_bundle_uuid}/bitstreams"
    path = "/home/manfredraffelsieper/etddepositor_project/processing_dir/license"
    
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
            except:
                print("wrong stuff just try")
    if os.path.isfile(fippa_agreement_path):
        with open(fippa_agreement_path, "rb") as file:
            try:
                license = {"file": ("fippa_agreement.txt", file, "text/plain")}
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
            except:
                print("put this in a method and fix later")

def upload_files(session, og_bundle_uuid, metadata_payload):

    original_endpoint = f"{API_BASE}/core/bundles/{og_bundle_uuid}/bitstreams"
    bitstream_metadata = []
    bitstream_path = "/home/manfredraffelsieper/etddepositor_project/processing_dir/ready/100775310_1839/data/"

    for file_name in os.listdir(bitstream_path):
        if file_name == "100775310jullm.pdf":

            mime_type = mimetypes.guess_type(file_name)[0]
            file_path = os.path.join(bitstream_path, file_name)  
            
            if os.path.isfile(file_path):   
                with open(file_path, "rb") as file:

                    if os.path.getsize(file_path) > 1048576000:

                        multipart_data = {
                            'file': (file_name, file),
                            'og_bitstream_payload': (None, json.dumps(metadata_payload), 'application/json')
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
                            response = session.safe_request("POST", original_endpoint, files=files, data=metadata_payload)
                            response.raise_for_status()
                            data = json.loads(response.text)
                        except requests.exceptions.RequestException as e:
                            logger.error(f"Error uploading {file_path}: {e}")
                            continue
                        print(data)
                    logger.info(f"Successfully uploaded: {file_path}") 
            else:
                logger.info(f"Skipping directory: {file_path}")
        else: 
            continue
        return data

def transfer_dspace(session, user, password):
    
    metadata_payload = {
    "name": "ETD Payload",
    "metadata": {
        "dc.title": [
            {
                "value": "On the otherside of the maze",
                "language": "en"
            }
        ],
        "dc.contributor.author": [
            {
                "value": "Hedge Runner",
                "language": "en"
            }
        ],
        "dc.identifier.doi": [
            {
                "value": "10.1234/example-doi.2025.001",
                "language": None
            }
        ]
    },
    "inArchive": True,
    "discoverable": True,
    "withdrawn": False,
    "type": "item"
    }
    try:
        session.authenticate(user, password)
        collection_id = "6500c3fd-86ef-4e24-aa40-7971ac850589"
        item_uuid = item_creation(session, collection_id, metadata_payload)
        og_bundle_uuid, license_bundle_uuid = bundle_creations(session, item_uuid)
        upload_licenses(session, license_bundle_uuid)
        upload_files(session, og_bundle_uuid, metadata_payload)
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

