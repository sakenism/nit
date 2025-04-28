import psycopg2
import os
from psycopg2 import sql
from datetime import datetime, date
import xlsxwriter

from dotenv import load_dotenv
load_dotenv()

class Postgre():
    def __init__(self) -> None:
        self.host = None
        self.user = None
        self.password = None
        self.port = None
        self.database = None
        self.connection = None

    def disconnect(self):
        self.host = None
        self.user = None
        self.password = None
        self.port = None
        self.database = None
        if self.connection is not None:
            self.connection.close()
        self.connection = None

    def connect(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database

        try:
            # Establish a connection to the database
            connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            self.connection = connection
            return connection

        except Exception as error:
            print(f"Error connecting to PostgreSQL database: {error}")
            return None

    
    def execute_query_params(self, query, params=(), query_type='select'):
        print(query)
        print(params)
        cursor = None
        if self.connection is None:
            self.connection = self.connect(self.host, self.user, self.password, self.port, self.database)
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            if query_type.lower() == 'select':
                results = cursor.fetchall()
                print(results)
                # Get column names from cursor description
                column_names = [desc[0] for desc in cursor.description]
                # Convert list of tuples to list of dictionaries
                results = [dict(zip(column_names, row)) for row in results]
                print("Query executed successfully.")
                return results
            else:
                self.connection.commit()
                print("Query executed successfully.")
                return True

        except Exception as error:
            print(f"Error executing query: {error}")
            return None

        finally:
            if cursor is not None:
                cursor.close()
            if self.connection is not None:
                self.connection.close()
                self.connection = None

host = os.getenv('NIT_DB_HOST')
user = os.getenv('NIT_DB_USER')
password = os.getenv('NIT_DB_PASSWORD')
port = os.getenv('NIT_DB_PORT')
database = os.getenv('NIT_DB_DATABASE')


psql = Postgre()
psql.connect(host, user, password, port, database)

query = '''
    select * from invest;
'''
# params = ('112',)
# response = psql.execute_query_params(query)
# for i, elem in enumerate(response):
#     for key in response[i]:
#         if isinstance(response[i][key], datetime) or isinstance(response[i][key], date):
#             response[i][key] = response[i][key].strftime("%Y-%m-%d")
#     print(elem)

# # Create a workbook and add a worksheet
# workbook = xlsxwriter.Workbook('invest_data.xlsx')
# worksheet = workbook.add_worksheet()

# # Write the headers
# if response:
#     headers = response[0].keys()
#     for col_num, header in enumerate(headers):
#         worksheet.write(0, col_num, header)

#     # Write the data
#     for row_num, data in enumerate(response, start=1):
#         for col_num, (key, value) in enumerate(data.items()):
#             worksheet.write(row_num, col_num, value)

# workbook.close()

#     id = elem['id']
#     document_title = elem['document_title']
#     document_body = elem['document_body']
#     lang = elem['lang']
#     created_at = elem['created_at']
#     status = elem['status']
#     requisites = elem['requisites']
#     print('id', id, '\n')
#     print('document_title', document_title, '\n')
#     print('document_body', document_body, '\n')
#     print('lang', lang, '\n')
#     print('created_at', created_at, '\n')
#     print('status', status, '\n')
#     print('requisites', requisites, '\n')

# print(len(response))