import os
import sys
import logging
from typing import List
import mysql.connector

class SQLQueryRunner:
    def __init__(self, connection_params: dict):
        """Initialize the query runner with database connection parameters."""
        self.connection_params = connection_params
        self.setup_logging()
    
    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sql_execution.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_sql_files(self, directory: str) -> List[str]:
        """Get all .sql files from the specified directory."""
        sql_files = []
        for file in os.listdir(directory):
            if file.endswith('.sql'):
                sql_files.append(os.path.join(directory, file))
        return sorted(sql_files)
    
    def read_query_from_file(self, file_path: str) -> str:
        """Read and return the SQL query from a file."""
        try:
            with open(file_path, 'r') as file:
                return file.read()
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def execute_query(self, query: str, file_name: str) -> bool:
        """Execute a single SQL query and return True if successful."""
        try:
            with mysql.connector.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    self.logger.info(f"Executing query from {file_name}")
                    
                    # Split multiple statements if present
                    statements = query.split(';')
                    statements = [stmt.strip() for stmt in statements if stmt.strip()]
                    
                    for statement in statements:
                        if statement:
                            cursor.execute(statement)
                            
                            # If it's a SELECT query, log the results
                            if statement.lower().strip().startswith('select'):
                                results = cursor.fetchall()
                                for row in results:
                                    self.logger.info(f"Result: {row}")
                    
                    conn.commit()
                    self.logger.info(f"Successfully executed query from {file_name}")
                    return True
        except Exception as e:
            self.logger.error(f"Error executing query from {file_name}: {str(e)}")
            return False
    
    def run_all_queries(self, directory: str):
        """Run all SQL queries from the specified directory in sequence."""
        sql_files = self.get_sql_files(directory)
        
        if not sql_files:
            self.logger.warning(f"No SQL files found in {directory}")
            return
        
        self.logger.info(f"Found {len(sql_files)} SQL files to execute")
        
        for sql_file in sql_files:
            try:
                query = self.read_query_from_file(sql_file)
                success = self.execute_query(query, os.path.basename(sql_file))
                
                if not success:
                    self.logger.error(f"Stopping execution due to failure in {sql_file}")
                    sys.exit(1)
                    
            except Exception as e:
                self.logger.error(f"Fatal error processing {sql_file}: {str(e)}")
                sys.exit(1)

def main():
    # MySQL connection parameters
    connection_params = {
        'host': 'localhost',
        'user': 'root',
        'password': '1234567890',
        'database': 'temp',
        'raise_on_warnings': True
    }
    
    # Directory containing SQL files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_directory = os.path.join(current_dir, 'sql_files')
    
    # Create sql_files directory if it doesn't exist
    os.makedirs(sql_directory, exist_ok=True)
    
    runner = SQLQueryRunner(connection_params)
    runner.run_all_queries(sql_directory)

if __name__ == "__main__":
    main()