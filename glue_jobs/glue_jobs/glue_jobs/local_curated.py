# glue_jobs/local_curated.py
# Reads processed Parquet from S3
# Creates 5 business KPI tables
# Writes curated Parquet back to S3
# Run: python glue_jobs/local_curated.py

import boto3, pandas as pd, os, io
from dotenv import load_dotenv
load_dotenv()

s3     = boto3.client('s3', region_name=os.getenv('AWS_REGION'))
BUCKET = os.getenv('ZIPIT_BUCKET')

def read_parquet(prefix, filename):
    obj = s3.get_object(
        Bucket=BUCKET,
        Key=f'processed/{prefix}/{filename}')
    return pd.read_parquet(
        io.BytesIO(obj['Body'].read()))

def write_parquet(df, prefix, filename):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)
    s3.put_object(
        Bucket=BUCKET,
        Key=f'curated/{prefix}/{filename}',
        Body=buf.getvalue())
    print(f'  Written to S3: curated/{prefix}/{filename}')

# Load all processed tables
print('Loading processed tables from S3...')
orders = read_parquet('orders',      'orders.parquet')
riders = read_parquet('riders',      'riders.parquet')
rests  = read_parquet('restaurants', 'restaurants.parquet')
print(f'  Orders loaded    : {len(orders):,} rows')
print(f'  Riders loaded    : {len(riders):,} rows')
print(f'  Restaurants loaded: {len(rests):,} rows')

# Fix numeric columns
orders['total_amount'] = pd.to_numeric(
    orders['total_amount'], errors='coerce').fillna(0)
orders['delivery_mins'] = pd.to_numeric(
    orders['delivery_mins'], errors='coerce')
orders['customer_rating'] = pd.to_numeric(
    orders['customer_rating'], errors='coerce')

# Only delivered orders for revenue analysis
delivered = orders[
    orders['status'] == 'delivered'].copy()
print(f'  Delivered orders : {len(delivered):,} rows')
print()

# ── 1. DAILY REVENUE BY CITY ──────────────────────────────
print('Building daily_revenue...')
delivered['order_date'] = pd.to_datetime(
    delivered['placed_at'],
    errors='coerce').dt.date

daily_rev = (delivered
    .groupby(['city', 'order_date'])
    .agg(
        total_revenue    =('total_amount', 'sum'),
        total_orders     =('order_id',     'count'),
        avg_order_value  =('total_amount', 'mean'),
        avg_delivery_mins=('delivery_mins','mean'),
        unique_customers =('customer_id',  'nunique'),
    )
    .reset_index()
)
daily_rev['total_revenue']     = daily_rev['total_revenue'].round(2)
daily_rev['avg_order_value']   = daily_rev['avg_order_value'].round(2)
daily_rev['avg_delivery_mins'] = daily_rev['avg_delivery_mins'].round(1)
daily_rev['order_date']        = daily_rev['order_date'].astype(str)
print(f'  Rows: {len(daily_rev)}')
write_parquet(daily_rev,
              'daily_revenue',
              'daily_revenue.parquet')

# ── 2. TOP RESTAURANTS ────────────────────────────────────
print('Building top_restaurants...')
rest_stats = (delivered
    .groupby('restaurant_id')
    .agg(
        total_orders    =('order_id',        'count'),
        total_gmv       =('total_amount',    'sum'),
        avg_order_value =('total_amount',    'mean'),
        avg_rating      =('customer_rating', 'mean'),
        avg_del_mins    =('delivery_mins',   'mean'),
    )
    .reset_index()
)
rest_stats['total_gmv']       = rest_stats['total_gmv'].round(2)
rest_stats['avg_order_value'] = rest_stats['avg_order_value'].round(2)
rest_stats['avg_rating']      = rest_stats['avg_rating'].round(2)
rest_stats['avg_del_mins']    = rest_stats['avg_del_mins'].round(1)

top_rests = rest_stats.merge(
    rests[['restaurant_id', 'name', 'city', 'cuisine']],
    on='restaurant_id', how='left'
).sort_values('total_orders', ascending=False)
print(f'  Rows: {len(top_rests)}')
write_parquet(top_rests,
              'top_restaurants',
              'top_restaurants.parquet')

# ── 3. RIDER PERFORMANCE ─────────────────────────────────
print('Building rider_performance...')
rider_del = delivered[
    delivered['rider_id'].notna() &
    (delivered['rider_id'] != '')
].copy()

rider_stats = (rider_del
    .groupby('rider_id')
    .agg(
        deliveries_done =('order_id',        'count'),
        avg_del_mins    =('delivery_mins',   'mean'),
        avg_rating      =('customer_rating', 'mean'),
        cities_served   =('city',            'nunique'),
    )
    .reset_index()
)
rider_stats['avg_del_mins'] = rider_stats['avg_del_mins'].round(1)
rider_stats['avg_rating']   = rider_stats['avg_rating'].round(2)

rider_perf = rider_stats.merge(
    riders[['rider_id', 'name', 'city', 'vehicle_type']],
    on='rider_id', how='left'
).sort_values('deliveries_done', ascending=False)
print(f'  Rows: {len(rider_perf)}')
write_parquet(rider_perf,
              'rider_performance',
              'rider_performance.parquet')

# ── 4. CANCELLATION ANALYSIS ─────────────────────────────
print('Building cancellation_analysis...')
cancel = (orders
    .groupby('city')
    .agg(
        total_orders    =('order_id', 'count'),
        cancelled_orders=('status',
            lambda x: (x == 'cancelled').sum()),
        delivered_orders=('status',
            lambda x: (x == 'delivered').sum()),
    )
    .reset_index()
)
cancel['cancel_rate_pct'] = (
    cancel['cancelled_orders'] * 100.0
    / cancel['total_orders']
).round(2)
cancel['delivery_success_pct'] = (
    cancel['delivered_orders'] * 100.0
    / cancel['total_orders']
).round(2)
print(f'  Rows: {len(cancel)}')
write_parquet(cancel,
              'cancellation_analysis',
              'cancellation.parquet')

# ── 5. PAYMENT ANALYSIS ───────────────────────────────────
print('Building payment_analysis...')
pay_orders = orders[
    orders['status'] != 'cancelled'].copy()
pay_orders['total_amount'] = pd.to_numeric(
    pay_orders['total_amount'],
    errors='coerce').fillna(0)

pay_anal = (pay_orders
    .groupby('payment_method')
    .agg(
        transaction_count=('order_id',      'count'),
        total_value      =('total_amount',  'sum'),
        avg_order_value  =('total_amount',  'mean'),
    )
    .reset_index()
    .sort_values('transaction_count', ascending=False)
)
pay_anal['total_value']     = pay_anal['total_value'].round(2)
pay_anal['avg_order_value'] = pay_anal['avg_order_value'].round(2)
print(f'  Rows: {len(pay_anal)}')
write_parquet(pay_anal,
              'payment_analysis',
              'payment_analysis.parquet')

print()
print('ALL 5 CURATED TABLES WRITTEN TO S3!')
print('  curated/daily_revenue/')
print('  curated/top_restaurants/')
print('  curated/rider_performance/')
print('  curated/cancellation_analysis/')
print('  curated/payment_analysis/')
print()
print(f'Check: https://s3.console.aws.amazon.com'
      f'/s3/buckets/{BUCKET}')