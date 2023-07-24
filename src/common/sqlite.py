"""SQLite DB connection"""

import sqlite3

class NotionDB:
    """
    Class to handle DB related operations
    """

    def __init__(self):
        """
        Initializes engine object
        """
        self._engine = sqlite3.connect('db/notion-data.db')

    def get_last_record(self):
        """
        Get Max date from raw data,
        If table empty, perform full load operation.
        
        This function will be called first before insert_raw_data method.
        """
        pass

    def insert_raw_data(self):
        """
        Method to insert Raw Notion API data
        """
        pass

    def calculate_company_questions(self):
        """
        Method to calculate, total questions solved
        company wise. Perform UPSERT operation
        """
        pass

    def calculate_daily_questions_solved(self):
        """
        Method to calculate, daily solved questions
        """
        pass
