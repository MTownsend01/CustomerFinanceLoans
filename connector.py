import pandas as pd
from sqlalchemy import create_engine, text
import yaml

def load_credentials(file_path):
    with open(file_path, 'r') as yaml_file:
        credentials = yaml.safe_load(yaml_file)
    return credentials

class RDSDatabaseConnector():
    def __init__(self, credentials):
        self.credentials = credentials
        self.engine = self.create_engine()

    def create_engine(self):
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
        query = f"SELECT * FROM {table_name};"
        
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            data_frame = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return data_frame
    
    def save_to_csv(self, data_frame, file_path):
        data_frame.to_csv(file_path, index=False)

if __name__ == '__main__':
    credentials = load_credentials('credentials.yaml')
    connector = RDSDatabaseConnector(credentials)

    data_frame = connector.extract_data('loan_payments')
    connector.save_to_csv(data_frame, 'loan_payments.csv')

def load_data():
    df = pd.read_csv('loan_payments.csv')

