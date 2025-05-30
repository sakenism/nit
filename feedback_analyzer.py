#!/usr/bin/env python3
import sqlite3
import pandas as pd
import os
import psycopg2

from psycopg2 import sql
import json
from datetime import datetime
import argparse
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

host = os.getenv('NIT_DB_HOST_ext')
user = os.getenv('NIT_DB_USER')
password = os.getenv('NIT_DB_PASSWORD')
port = os.getenv('NIT_DB_PORT')
database = os.getenv('NIT_DB_DATABASE')
print(host, user, password, port, database)

psql = Postgre()
psql.connect(host, user, password, port, database)

class SQLiteAnalyzer:
    def __init__(self, db_path):
        """Initialize connection to SQLite database"""
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        # Set row factory to access results by column name
        self.conn.row_factory = sqlite3.Row
        
    def __del__(self):
        """Close connection when object is destroyed"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    def execute_query(self, query, params=None):
        """
        Execute a SQL query with optional parameters
        
        Args:
            query: SQL query string
            params: Parameters for the query (tuple, list, or dict)
            
        Returns:
            List of dictionaries with column-name access
        """
        cursor = self.conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # Convert row objects to dictionaries for easy column name access
            results = [dict(row) for row in cursor.fetchall()]
            return results
        except sqlite3.Error as e:
            print(f"Query error: {e}")
            print(f"Query: {query}")
            print(f"Params: {params}")
            return None
    
    def get_tables(self):
        """Get list of all tables in the database"""
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        results = self.execute_query(query)
        return [row['name'] for row in results]
    
    def get_columns(self, table_name):
        """Get column information for a specific table"""
        query = f"PRAGMA table_info({table_name});"
        return self.execute_query(query)
    
    def to_dataframe(self, query, params=None):
        """
        Execute query and return results as pandas DataFrame
        
        Args:
            query: SQL query string
            params: Parameters for the query (tuple, list, or dict)
            
        Returns:
            pandas DataFrame with query results
        """
        try:
            if params:
                df = pd.read_sql_query(query, self.conn, params=params)
            else:
                df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            print(f"DataFrame conversion error: {e}")
            return None
    
    def save_results(self, results, output_path=None, format="csv"):
        """Save query results to file"""
        if not results:
            print("No results to save")
            return
            
        if not output_path:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            output_path = f"/output/results_{timestamp}.{format}"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Convert results to DataFrame
        if isinstance(results, list):
            df = pd.DataFrame(results)
        else:
            df = results
            
        # Save based on format
        if format.lower() == "csv":
            df.to_csv(output_path, index=False)
        elif format.lower() == "json":
            df.to_json(output_path, orient="records", indent=4)
        elif format.lower() == "excel":
            df.to_excel(output_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
            
        print(f"Results saved to {output_path}")


# Example usage
if __name__ == "__main__":
    path = 'webui.db'
    query = 'select * from feedback order by created_at desc';
    analyzer = SQLiteAnalyzer(path)
    
    # Example 1: Get all tables
    tables = analyzer.get_tables()
    print(f"Tables in database: {tables}")
    
    # Example 2: Query with parameters (safely handling user input)
    results = analyzer.execute_query(query)
    # results = analyzer.execute_query(
    #     "SELECT * FROM users WHERE id > ? AND status = ?", 
    #     (min_id, 'active')
    # )
    
    # Example 3: Access by column name
    if results:
        for row in results:  # Show first 3 results
            try:
                id = row['id']
                user_id = row['user_id']
                version = row['version']
                type = row['type']
                data = json.loads(row['data'])
                meta = json.loads(row['meta'])
                snapshot = json.loads(row['snapshot'])
                created_at = row['created_at']
                updated_at = row['updated_at']
                print('id', id)
                print('user_id', user_id)
                # print('version', version)
                print('type', type)
                # print('data', data)
                # print('meta', meta)
                # print('snapshot', snapshot)
                print('created_at', created_at)
                print('updated_at', updated_at)

                action = 'like' if data['rating'] == 1 else 'dislike' if data['rating'] == -1 else None
                title = snapshot['chat']['chat']['title']
                history = snapshot['chat']['chat']['history']['messages']
                reason = data['reason']
                comment = data['comment']
                tags = data['tags']
                rating = data['details']['rating']
                messages = []

                for elem in history:
                    content = history[elem]['content']
                    messages.append(content.replace('\'', '\'\''))

                model_id = meta['model_id']
                message_id = meta['message_id']
                chat_id = meta['chat_id']
                
                print('action', action)
                print('title', title)
                print('chat_id', chat_id)
                # print('history', history)
                print('model_id', model_id)
                print('message_id', message_id)
                print('messages', messages)
                print('reason', reason)
                print('comment', comment)
                print('tags', tags)
                print('rating', rating)
                print('--------------------------------')
                insert_query = '''
                    INSERT INTO feedback_open_webui (
                    id,
                    user_id,
                    type,
                    created_at,
                    updated_at,
                    action,
                    title,
                    chat_id,
                    model_id,
                    message_id,
                    messages,
                    reason,
                    comment,
                    tags,
                    rating
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );'''
                
                params = (
                    id,
                    user_id,
                    type,
                    created_at,
                    updated_at,
                    action,
                    title,
                    chat_id,
                    model_id,
                    message_id,
                    ',,, '.join(messages),
                    reason,
                    comment,
                    ',,, '.join(tags),
                    rating
                )
                
                print(insert_query % params)
                psql.execute_query_params(insert_query, params=params, query_type='insert')
            except Exception as err:
                print('ERROR:', err)

    
query = '''
    select * from feedback_open_webui;
'''
# params = ('112',)
response = psql.execute_query_params(query)
print(response)



