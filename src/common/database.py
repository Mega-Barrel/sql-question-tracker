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

    def get_raw_data_max_date(self):
        """
        Get Max date from raw data table.
        """
        curr_max_date = text('SELECT MAX(DATE(created_at)) FROM raw_data;')
        max_date = self._connection.execute(curr_max_date)
        max_date = str(max_date.first()[0]).split(' ', maxsplit=1)[0]
        return max_date

    def get_max_date_record(self):
        """
        This method returns the most recent created_at date.

        It will return default date '2022-01-01' (as specified in Schema), 
            if the script is running for the 1st time.

        """
        query = text('SELECT created_at FROM max_date')
        data = self._connection.execute(query)
        # return date value
        return str(data.first()[0]).split(' ', maxsplit=1)[0]

    def update_max_date_record(self):
        """
        Update Max date, to new date
        
        :param new_date: Update date with latest entry in raw_table
        """
        date = self.get_raw_data_max_date()

        query = text(
            f"""
                UPDATE
                    max_date
                SET
                    created_at = '{date}'
                ;
            """
        )
        # Execute above query
        self._connection.execute(query)
        self._connection.commit()

    def calculate_company_questions(self):
        """
        Method to calculate, total questions solved
        company wise. Perform UPSERT operation
        """
        # Get last update date
        date = self.get_max_date_record()

        query = text(
            f"""
                WITH raw_company AS (
                    SELECT
                        unnest(company) AS company
                    FROM
                        raw_data
                    WHERE
                        DATE(created_at) > '{date}'
                )
                ,
                new_entries AS (
                    SELECT
                        company,
                        COUNT(company) AS question_solved
                    FROM
                        raw_company
                    GROUP BY
                        1
                )

                MERGE INTO
                    companies_solved cs
                USING
                    new_entries ne
                ON
                    cs.company = ne.company
                WHEN MATCHED
                    THEN
                    UPDATE SET 
                        question_solved = cs.question_solved + ne.question_solved
                WHEN NOT MATCHED THEN
                INSERT (company, question_solved)
                values (ne.company, ne.question_solved)
            ;
            """
        )
        # Execute above query
        self._connection.execute(query)
        self._connection.commit()

    def calculate_daily_questions_solved(self):
        """
        Method to calculate, daily solved questions
        """

    def calculate_difficulty_level_questions(self):
        """
        Method to calculate, total questions solved by difficulty level
        
        :param date: Get the max date
        """
        # Get last update date
        date = self.get_max_date_record()

        # SQL Query
        query = text(
            f"""
            WITH new_entries AS (
                SELECT
                    difficulty,
                    COUNT(difficulty) AS question_solved
                FROM
                    raw_data
                WHERE
                    DATE(created_at) > '{date}'
                GROUP BY
                    1
            )

            MERGE INTO
                ques_difficulty qd
            USING
                new_entries ne
            ON
                qd.difficulty = ne.difficulty
            WHEN MATCHED
                THEN
                UPDATE SET 
                    question_solved = qd.question_solved + ne.question_solved
            WHEN NOT MATCHED THEN
            INSERT (difficulty, question_solved)
            values (ne.difficulty, ne.question_solved);
            ;
            """
        )
        # Execute above query
        self._connection.execute(query)
        self._connection.commit()
