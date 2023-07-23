"""NOTION DataBase API Endpoint"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv

class NSEtl():

    def __init__(self):
        load_dotenv()

        self._NOTION_API = os.environ.get('NOTION_API')
        self._DATABASE_ID = os.environ.get('DATABASE_ID')
        self.PAGE_SIZE = 100
        
        # API header
        self.headers = {
            "Authorization": f"Bearer {self._NOTION_API}",
            "accept": "application/json",
            "Notion-Version": "2022-06-28"
        }
    
    def extract(self):
        # Get database data
        url = f"https://api.notion.com/v1/databases/{self._DATABASE_ID}/query"

        payload = {"page_size": self.PAGE_SIZE}
        resp = requests.post(url, json=payload, headers=self.headers)

        if resp.status_code == 200:
            data = resp.json()['results']

            for attributes in data:
                question = attributes['properties']['Questions']['title'][0]['text']['content']
                difficulty = attributes['properties']['Tags']['select']['name']
                created_time = datetime.strptime(attributes['created_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
                platform = attributes['properties']['Platform']['select']['name']
                company = [{'Company': item["name"]} for item in attributes['properties']['Company']["multi_select"]]

                ndict = {
                    'Question Title': question,
                    'Dfficulty leve': difficulty,
                    'Created Time': created_time,
                    'Platform Solved': platform,
                    'Company Tag': company
                }

                print(ndict)
                print()
