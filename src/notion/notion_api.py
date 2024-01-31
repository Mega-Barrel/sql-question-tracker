"""NOTION DataBase API Endpoint"""

import os
from datetime import datetime

import requests
from dotenv import load_dotenv

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

        # Creating NotionDB object
        self.db = ''

        # API header
        self.headers = {
            "Authorization": f"Bearer {self._notion_api}",
            "accept": "application/json",
            "Notion-Version": "2022-06-28"
        }

    def process_data(self, json_data, data_list):
        """
        Method to pre-process JSON data
        
        :param json_data: RAW JSON data
        :param data_list: Processed dict data
        """
        # print(json_data)
        for attributes in json_data:
            page_id = attributes['id']
            question = attributes['properties']['Questions']['title'][0]['text']['content']
            difficulty = attributes['properties']['Difficulty']['select']['name']
            created_time = datetime.strptime(attributes['created_time'], self.date_format)
            status = attributes['properties']['Status']['status']['name']
            platform = attributes['properties']['Platform']['select']['name']
            company = [
                item["name"] for item in attributes['properties']['Company']["multi_select"]
            ]
            question_type = attributes['properties']['question_type']['select']['name']
            question_link = attributes['properties']['question_link']['url']
            page_url = attributes['url']

            ndict = {
                'page_id': page_id,
                'question_title': question,
                'difficulty': difficulty,
                'created_at': created_time,
                'platform': platform,
                'company': company,
                'question_type': question_type,
                'question_link': question_link,
                'question_status': status,
                'page_url': page_url
            }

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
                    "after": "2023-01-01"
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
