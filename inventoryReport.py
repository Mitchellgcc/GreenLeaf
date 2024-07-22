import requests
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
import os
from GetAccessToken import get_access_token  # Ensure this module is available
import yaml
from logging.handlers import RotatingFileHandler

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Ensure log directory exists
log_file_path = os.path.expanduser('~/PlaneHealth/logs/inventory_fetch.log')
log_dir = os.path.dirname(log_file_path)
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging_config = config['logging']
handlers = []
if 'console' in logging_config['handlers']:
    handlers.append(logging.StreamHandler())

if 'file' in logging_config['handlers']:
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=logging_config['file']['maxBytes'],
        backupCount=logging_config['file']['backupCount']
    )
    handlers.append(file_handler)

logging.basicConfig(
    level=logging_config['level'],
    format=logging_config['format'],
    handlers=handlers
)

def fetch_fba_inventory_report():
    logging.info("Fetching FBA inventory report...")
    access_token = get_access_token()
    logging.debug(f"Access Token: {access_token}")

    headers = {
        'x-amz-access-token': access_token,
        'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
        'Content-Type': 'application/json',
    }
    
    endpoint = "https://sellingpartnerapi-eu.amazon.com/reports/2020-09-04/reports"
    payload = {
        "reportType": "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA",
        "marketplaceIds": [config['amazon_api']['marketplace_id']],
        "reportOptions": {
            "scheduleStartDate": (datetime.utcnow() - timedelta(days=1)).isoformat()
        }
    }

    response = requests.post(endpoint, headers=headers, json=payload)
    logging.debug(f"Report Creation Response Status Code: {response.status_code}")
    logging.debug(f"Report Creation Response Data: {response.json()}")

    if response.status_code == 200:
        report_id = response.json().get('reportId')
        logging.info(f"Report created successfully with report ID: {report_id}")
        return report_id
    else:
        logging.error(f"Failed to create report. Status code: {response.status_code}")
        logging.error(f"Response Data: {response.json()}")
        return None

def get_report_document(report_id):
    logging.info(f"Getting report document for report ID: {report_id}")
    access_token = get_access_token()
    headers = {
        'x-amz-access-token': access_token,
        'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
        'Content-Type': 'application/json',
    }

    endpoint = f"https://sellingpartnerapi-eu.amazon.com/reports/2020-09-04/reports/{report_id}"
    response = requests.get(endpoint, headers=headers)
    logging.debug(f"Report Document Response Status Code: {response.status_code}")
    logging.debug(f"Report Document Response Data: {response.json()}")

    if response.status_code == 200:
        report_document_id = response.json().get('reportDocumentId')
        return report_document_id
    else:
        logging.error(f"Failed to get report document. Status code: {response.status_code}")
        logging.error(f"Response Data: {response.json()}")
        return None

def download_report(report_document_id):
    logging.info(f"Downloading report for document ID: {report_document_id}")
    access_token = get_access_token()
    headers = {
        'x-amz-access-token': access_token,
        'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
        'Content-Type': 'application/json',
    }

    endpoint = f"https://sellingpartnerapi-eu.amazon.com/reports/2020-09-04/documents/{report_document_id}"
    response = requests.get(endpoint, headers=headers)
    logging.debug(f"Download Report Response Status Code: {response.status_code}")
    logging.debug(f"Download Report Response Data: {response.json()}")

    if response.status_code == 200:
        report_url = response.json().get('url')
        logging.info(f"Report download URL: {report_url}")
        
        report_response = requests.get(report_url)
        if report_response.status_code == 200:
            return report_response.text
        else:
            logging.error(f"Failed to download report. Status code: {report_response.status_code}")
            logging.error(f"Response Data: {report_response.text}")
            return None
    else:
        logging.error(f"Failed to get report download URL. Status code: {response.status_code}")
        logging.error(f"Response Data: {response.json()}")
        return None

def main():
    report_id = fetch_fba_inventory_report()
    if report_id:
        report_document_id = get_report_document(report_id)
        if report_document_id:
            report_data = download_report(report_document_id)
            if report_data:
                # Assuming the report is CSV formatted
                report_df = pd.read_csv(pd.compat.StringIO(report_data))
                logging.info("Report data fetched successfully")
                print(report_df.head())  # Display the first few rows of the dataframe

if __name__ == "__main__":
    main()
