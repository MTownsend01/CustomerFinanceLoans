import pandas as pd

def load_data_from_csv(file_path):
    data = pd.read_csv(file_path)
    return data

if __name__ =='__main__':
    df = load_data_from_csv('loan_payments.csv')



print(df.info())
    