import boto3, pandas as pd, os, io
from dotenv import load_dotenv
load_dotenv()

s3     = boto3.client('s3', region_name=os.getenv('AWS_REGION'))
BUCKET = os.getenv('ZIPIT_BUCKET')

def read_s3_csv(prefix, filename):
    obj = s3.get_object(Bucket=BUCKET, Key=f'raw/{prefix}/{filename}')
    return pd.read_csv(io.BytesIO(obj['Body'].read()))

def write_s3_parquet(df, prefix, filename):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=f'processed/{prefix}/{filename}', Body=buf.getvalue())
    print(f'  Written: processed/{prefix}/{filename}')

print('Processing orders...')
orders = read_s3_csv('orders', 'orders.csv')
orders.dropna(subset=['order_id','total_amount'], inplace=True)
orders.drop_duplicates(subset=['order_id'], inplace=True)
orders['total_amount'] = pd.to_numeric(orders['total_amount'], errors='coerce').fillna(0)
orders['city'] = orders['city'].str.upper().str.strip()
print(f'  Orders: {len(orders):,} rows')
write_s3_parquet(orders, 'orders', 'orders.parquet')

print('Processing customers...')
custs = read_s3_csv('customers', 'customers.csv')
custs.drop_duplicates(subset=['customer_id'], inplace=True)
write_s3_parquet(custs, 'customers', 'customers.parquet')

print('Processing restaurants...')
rests = read_s3_csv('restaurants', 'restaurants.csv')
rests.drop_duplicates(subset=['restaurant_id'], inplace=True)
rests['rating'] = pd.to_numeric(rests['rating'], errors='coerce')
rests = rests[(rests['rating']>=1.0)&(rests['rating']<=5.0)]
write_s3_parquet(rests, 'restaurants', 'restaurants.parquet')

print('Processing riders...')
riders = read_s3_csv('riders', 'riders.csv')
riders.drop_duplicates(subset=['rider_id'], inplace=True)
write_s3_parquet(riders, 'riders', 'riders.parquet')

print('Processing payments...')
pays = read_s3_csv('payments', 'payments.csv')
pays.drop_duplicates(subset=['payment_id'], inplace=True)
write_s3_parquet(pays, 'payments', 'payments.parquet')

print('\nALL 5 TABLES PROCESSED AND IN S3!')