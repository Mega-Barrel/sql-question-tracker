"""DB connection"""

from sqlalchemy import text
from sqlalchemy import create_engine

class NotionDB:
    """
    Class to handle DB related operations
    """

    def __init__(self):
        """
        Initializes engine object
        """
        # self._engine = sqlite3.connect('db/notion-data.db')
        self.engine = create_engine('postgresql://postgres:joshi24@localhost:5432/notion-etl')
        self._connection = self.engine.connect()

    def get_last_record(self):
        """
        Get Max date from raw data,
        If table empty, perform full load operation.
        
        This function will be called first before insert_raw_data method.
        """
        query = text('SELECT created_at FROM max_date')
        data = self._connection.execute(query)

        # return date value
        return data.first()[0]

    def calculate_company_questions(self):
        """
        Method to calculate, total questions solved
        company wise. Perform UPSERT operation
        """

    def calculate_daily_questions_solved(self):
        """
        Method to calculate, daily solved questions
        """