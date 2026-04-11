# glue_jobs/check_types.py
# Reads actual Parquet files and prints exact column types
# Run: python glue_jobs/check_types.py

import boto3, pandas as pd, os, io
from dotenv import load_dotenv
load_dotenv()

s3     = boto3.client('s3', region_name=os.getenv('AWS_REGION'))
BUCKET = os.getenv('ZIPIT_BUCKET')

def check_parquet(prefix, filename):
    print(f'\n── {prefix}/{filename} ──────────────────────')
    try:
        obj = s3.get_object(Bucket=BUCKET,
                            Key=f'processed/{prefix}/{filename}')
        df  = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        for col, dtype in df.dtypes.items():
            # Map pandas dtype to Athena type
            if 'int' in str(dtype):
                athena = 'BIGINT'
            elif 'float' in str(dtype):
                athena = 'DOUBLE'
            elif 'datetime' in str(dtype):
                athena = 'TIMESTAMP'
            elif 'bool' in str(dtype):
                athena = 'BOOLEAN'
            else:
                athena = 'STRING'
            print(f'  {col:<25} {str(dtype):<15} → Athena: {athena}')
    except Exception as e:
        print(f'  ERROR: {e}')

check_parquet('orders',      'orders.parquet')
check_parquet('customers',   'customers.parquet')
check_parquet('restaurants', 'restaurants.parquet')
check_parquet('riders',      'riders.parquet')
check_parquet('payments',    'payments.parquet')