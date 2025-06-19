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


def submission_creation(session, collection_id):
    
    submission_endpoint = f"{API_BASE}/submission/workspaceitems?owningCollection={collection_id}"
    headers = {"Content-Type": "application/json"}
    
    response = session.safe_request("POST", submission_endpoint, headers=headers)
    response.raise_for_status()
    submission_id = response.json()["id"] 
    print("created submission with id:", submission_id)
    return submission_id

def get_current_submission(session, submission_id):
    endpoint = f"{API_BASE}/submission/workspaceitems/{submission_id}"
    response = session.safe_request("GET", endpoint)
    response.raise_for_status()

    submission_data = response.json()
    print("SUBMISSION DETAILS", submission_data)
    return submission_data

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
                "value": "Test Submission Title",
                "language": None,
                "authority": None,
                "confidence": 500
            }]
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
    
def upload_files(session, submission_id, file_path, bundle="ORIGINAL", description=None):
    endpoint = f"{API_BASE}/submission/workspaceitems/{submission_id}"

    file_name = os.path.basename(file_path)

    files = {
        "file": (file_name, open(file_path, "rb")),
        "bundleName": (None, bundle)
    }

    if description:
        files["description"] = (None, description)


    try:
        
        response = session.safe_request("POST", endpoint, files=files)
        response.raise_for_status()
        print(f"Uploaded {file_name} to bundle '{bundle}':", response.json())
        return response.json()
    except requests.exceptions.HTTPError as e:
        print("Error adding metadata:", e)
        return None
    
def finalize_submission(session, submission_id):
    endpoint = f"{API_BASE}/submission/workflowitems"
    data = f"{API_BASE}/submission/workspaceitems/{submission_id}"

    headers = {
        "Content-Type": "text/uri-list"
    }

    try:
        response = session.safe_request("POST", endpoint, headers=headers, data=data)
        response.raise_for_status()
        print("Submission finalized:", response.json())
        return response.json()["id"]
    except requests.exceptions.HTTPError as e:
        print("Error finalizing submission:", e.response.text)
        return None
    
def submission_workflow(session, collection_id):
    file_path = "/home/manfredraffelsieper/etddepositor_project/processing_dir/ready/100775310_1839/data/100775310jullm.pdf"
    zip_path = "/home/manfredraffelsieper/test.zip"
    fippa_path = "/home/manfredraffelsieper/fippa_statement.txt"
    submission_id = 44280
    #submission_id = submission_creation(session, collection_id)
    
    add_metadata(session, submission_id)
    upload_files(session, submission_id,  file_path, bundle="ORIGINAL")
    upload_files(session, submission_id, zip_path, bundle="SUPPLEMENTAL")
    upload_files(session, submission_id, fippa_path, bundle="LICENSE")
    workflowitem_id = finalize_submission(session, submission_id)
    
    
def transfer_dspace(session, user, password):
    
    try:
        session.authenticate(user, password)

        submission_workflow(session, "69d095be-875d-4ee2-b3b8-4e11015d09be")
        
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

