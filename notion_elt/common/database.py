"""DB connection"""

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

    def get_last_record(self):
        """
        Get Max date from raw data,
        If table empty, perform full load operation.
        
        This function will be called first before insert_raw_data method.
        """

    def calculate_company_questions(self):
        """
        Method to calculate, total questions solved
        company wise. Perform UPSERT operation
        """

    def calculate_daily_questions_solved(self):
        """
        Method to calculate, daily solved questions
        """
