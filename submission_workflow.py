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
password = ""

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

def add_metadata_field(session, submission_id, metadata_key):

# Step 1: Get the submission workspace item to find the correct section name
    endpoint = f"{API_BASE}/submission/workspaceitems/{submission_id}"
    try:
        response = session.safe_request("GET", endpoint)
        response.raise_for_status()
        data = response.json()

        # Extract section name (assumes there's only one, or uses the first if multiple)
        sections = data.get("sections", {})
        if not sections:
            raise ValueError("No submission form sections found in the workspace item.")
        metadata_section_name = next(
            (name for name in sections.keys() if name.lower().startswith("traditionalpageone") or "dc." in str(sections[name])), 
            None
        )

        if not metadata_section_name:
            raise ValueError("Could not identify the metadata section. Available sections: " + ", ".join(sections.keys()))


    except Exception as e:
        print("Failed to retrieve workspace item or section name.")
        print("Error:", str(e))
        return

    # Step 2: Construct JSON Patch payload
    patch_payload = {
        "value": "Hello from WSL",
        "language": "en_CA"
    }



    # Step 3: Send PATCH to add metadata
    try:
        patch_endpoint = f"{API_BASE}/submission/workspaceitems/{submission_id}/sections/{metadata_section_name}/{metadata_key}"
        headers = { "Content-Type" : "application/json"}
        patch_response = session.safe_request("POST",  patch_endpoint, json=patch_payload, headers=headers)
        patch_response.raise_for_status()
        return patch_response
    except requests.exceptions.HTTPError as e:
        print("Failed to PATCH metadata.")
        print("Status code:", patch_response.status_code)
        print("Response body:", patch_response.text)
    


def add_metadata(session, submission_id):
    metadata_payload = [
        {
            "op": "add",
            "path": "/sections/traditionalpageone-carleton/dc.title",
            "value": {
                "value": "Test Submission Title",
                "language": None,
                "authority": None,
                "confidence": 500
            }
        },
        {
            "op": "add",
            "path": "/sections/traditionalpageone-carleton/dc.date.issued",
            "value": {
                "value": "2025-06-12",
                "language": None,
                "authority": None,
                "confidence": 500
            }
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
    
def submission_workflow(session, collection_id):

    sub_id = 44067
    #submission_id = submission_creation(session, collection_id)
    response = get_current_submission(session, sub_id)
    
    add_metadata(session, sub_id)
    #add_metadata_field(session, sub_id, "dc.title")
    print(json.dumps(response.json(), indent=2))
    
    
def transfer_dspace(session, user, password):
    
    try:
        session.authenticate(user, password)

        submission_workflow(session, "e0dc7bb9-9a07-454a-befb-42a434591436")
        
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

