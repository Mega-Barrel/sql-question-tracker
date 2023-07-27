"""NOTION DataBase API Endpoint"""

import os
from datetime import datetime

import requests
import pandas as pd
from dotenv import load_dotenv

from src.common.database import NotionDB

class NSEtl():
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
        self.db = NotionDB()

        # API header
        self.headers = {
            "Authorization": f"Bearer {self._notion_api}",
            "accept": "application/json",
            "Notion-Version": "2022-06-28"
        }

    def extract(self):
        """
        Method to Extract Notion Database data.
        """
        url = f"https://api.notion.com/v1/databases/{self._database_id}/query"
        # Specify page_size and database filtering
        payload = {
            "page_size": self.page_size,
            "filter": { 
                "timestamp": "created_time",
                "created_time": {
                    "after": self.db.get_max_date_record()
                }
            }
        }

        resp = requests.post(url, json=payload, headers=self.headers, timeout=5000)

        if resp.status_code == 200:
            data = resp.json()['results']
            raw_data = []

            for attributes in data:
                question = attributes['properties']['Questions']['title'][0]['text']['content']
                difficulty = attributes['properties']['Tags']['select']['name']
                created_time = datetime.strptime(attributes['created_time'], self.date_format)
                platform = attributes['properties']['Platform']['select']['name']
                company = [
                    item["name"] for item in attributes['properties']['Company']["multi_select"]
                ]

                ndict = {
                    'question_title': question,
                    'difficulty': difficulty,
                    'created_at': created_time,
                    'platform': platform,
                    'company': company
                }

                raw_data.append(ndict)
        # Return raw_data list
        return raw_data

    def load(self, data: pd.DataFrame):
        """
        Method to save DataFrame to Raw Table
        """
        data_frame = pd.DataFrame(data)
        data_frame.to_sql(
            'raw_data', 
            self.db.engine,
            if_exists='append',
            index=False
        )

    def transform(self):
        """
        Method to apply transformations to data,
        and save to new table
        """
        # Update date with latest record
        print(f'Pre Update: {self.db.get_max_date_record()}')
        self.db.calculate_company_questions()
        self.db.calculate_difficulty_level_questions()

        # Update table with latest date
        self.db.update_max_date_record()
        print(f'Post Update: {self.db.get_max_date_record()}')

    def elt_process(self):
        """
        Main Method to call Extract, Load, and Transform functions
        """
        # Extract
        data_frame = self.extract()
        # Load
        self.load(data=data_frame)
        # Transform
        self.transform()
