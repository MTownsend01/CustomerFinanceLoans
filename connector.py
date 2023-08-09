import pandas as pd
from sqlalchemy import create_engine, text
import yaml

def load_credentials(file_path):
    '''
    Loads database credentials from a YAML file.
    
    Args:
        file_path (str): Path to the credentials YAML file.
        
    Returns:
        dict: Dictionary containing the loaded credentials.
    '''
    with open(file_path, 'r') as yaml_file:
        credentials = yaml.safe_load(yaml_file)
    return credentials

class RDSDatabaseConnector():
    '''
    A class to manage database connections, data extraction, and saving to CSV
    
    Attributes:
        credentials (dict): Dictionary containing database credentials
        
    Methods:
        __init__(self, credentials): Initialises the RDSDatabaseConnector instance.
        create_engine(self): Creates a SQLAlchemy engine.
        extract_data(self, table_name): Extracts data from the database table.
        save_to_csv(self, data_frame, file_path): Saves DataFrame to a CSV file.
    '''

    def __init__(self, credentials):
        self.credentials = credentials
        self.engine = self.create_engine()

    def create_engine(self):
        '''
        Creates a SQLAlchemy engine based on provided credentials.
        
        Returns:
            engine: SQLAlchemy engine object.
        '''
        connection_string = f"postgresql://{self.credentials['RDS_USERNAME']}:{self.credentials['RDS_PASS']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_NAME']}"
        engine = create_engine(connection_string)
        return engine
    
    def fetch_data(self, query):
        engine = self.create_engine()

        with engine.connect() as connection:
            result = connection.execute(query)
            data = result.fetchall()

        loans_payments = pd.DataFrame(data, columns=result.keys())
        return loans_payments
    
    def extract_data(self, table_name):
        '''Extracts data from the specified database table.
        
        Args:
            table_name (str): Name of the table to extract data from.
            
        Returns:
            pd.DataFrame: Pandas Dataframe containing the extracted data.
        '''
        query = f"SELECT * FROM {table_name};"
        
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            data_frame = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return data_frame
    
    def save_to_csv(self, data_frame, file_path):
        '''
        Saves a Pandas DataFrame to a CSV file.
        
        Args:
            data_frame (pd.DataFrame): DataFrame to be saved.
            file_path (str): Path to the output CSV file.
        '''
        data_frame.to_csv(file_path, index=False)

if __name__ == '__main__':
    credentials = load_credentials('credentials.yaml')
    connector = RDSDatabaseConnector(credentials)

    data_frame = connector.extract_data('loan_payments')
    connector.save_to_csv(data_frame, 'loan_payments.csv')

def load_data():
    df = pd.read_csv('loan_payments.csv')

