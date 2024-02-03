"""NOTION DataBase API Endpoint"""

# System Packages
import os
from datetime import datetime

# Installed Packages
import requests
from dotenv import load_dotenv

# Folder imports
from src.bigquery.database import BigQueryOperations

class NotionWrapper():
    """
    Notion ELT Class
    """

    def __init__(self):
        """
        Class Constructor
        """
        load_dotenv()

        self._notion_api = os.environ.get('NOTION_API')
        self._database_id = os.environ.get('DATABASE_ID')
        self.date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        self.page_size = 100

        # Creating DB object
        self.dataset_name = 'coding_question'
        self.table_name = 'stg_raw_questions'
        self.db = BigQueryOperations(
            dataset_name=self.dataset_name,
            table_name=self.table_name
        )
        self.dt = '1990-01-01' if self.db.get_max_date() is None else self.db.get_max_date()

        # API header
        self.headers = {
            "Authorization": f"Bearer {self._notion_api}",
            "accept": "application/json",
            "Notion-Version": "2022-06-28"
        }

    def process_data(self, json_data, data_list: list[dict]) -> list[dict]:
        """
        Method to pre-process JSON data
        
        :param json_data: RAW JSON data
        :param data_list: Processed dict data
        """
        # print(json_data)
        for result in json_data:
            # Extract company information
            if "properties" in result and "Company" in result["properties"]:
                companies = result["properties"]["Company"]["multi_select"]
                company_names = [company["name"] for company in companies]
                company_str = ' | '.join(company_names)

            ndict = {
                'page_id': result.get('id', ''),
                'question_title': result['properties']['Questions']['title'][0]['text']['content'],
                'difficulty': result['properties']['Difficulty']['select']['name'],
                'created_at': str(datetime.strptime(result['created_time'], self.date_format)),
                'platform': result['properties']['Platform']['select']['name'],
                'company': company_str,
                'question_type': result['properties']['question_type']['select']['name'],
                'question_link': result['properties']['question_link']['url'],
                'question_status': result['properties']['Status']['status']['name'],
                'page_url': result['url']
            }

            # Append dict to list
            data_list.append(ndict)

    def extract(self):
        """
        Method to Extract Notion Database data.
        """
        list_data = []
        url = f"https://api.notion.com/v1/databases/{self._database_id}/query"
        # Specify page_size and database filtering
        payload = {
            "page_size": self.page_size,
            "filter": { 
                "timestamp": "created_time",
                "created_time": {
                    "after": f"{self.dt}"
                }
            }
        }

        # Request to Notion Endpoint
        resp = requests.post(url, json=payload, headers=self.headers, timeout=5000)
        if resp.status_code == 200:
            data = resp.json()
            results = data['results']

            while data['has_more']:
                # Update has_more to next_cursor, if has_more is True
                payload['start_cursor'] = data['next_cursor']
                resp = requests.post(url, json=payload, headers=self.headers, timeout=5000)

                # Check if status_code == 200
                if resp.status_code == 200:
                    data = resp.json()
                    # Add pagination data to clean_data
                    results.extend(data['results'])

            self.process_data(results, list_data)
            # return clean_data list
            return list_data

    def load_data(self, raw_data: list[dict]):
        """ Passes the list data to BigQueryOperations class, which handles all DB processes.

        Args:
            data: list[dict], List of dictionaries to insert
        """
        if raw_data:
            print('Data available to insert.')
            self.db.insert_data(rows_to_insert=raw_data)
        else:
            print('No data available to insert.')
