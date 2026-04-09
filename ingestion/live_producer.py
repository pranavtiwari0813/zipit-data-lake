# ingestion/live_producer.py
# Simulates live ZIPIT order events
# Writes directly to S3 raw/orders/ every few seconds
# This replaces Kinesis for Free Tier accounts

import boto3, json, random, time, uuid, os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

s3     = boto3.client('s3',
           region_name=os.getenv('ap-south-1'))
BUCKET = os.getenv('zipit-datalake-engineer')

CITIES = ['Delhi','Mumbai','Bangalore',
          'Pune','Hyderabad','Chennai','Kolkata']
FOODS  = [
    ('Chicken Biryani',      320),
    ('Paneer Butter Masala', 280),
    ('Veg Pizza',            250),
    ('Masala Dosa',          120),
    ('Chicken Burger',       150),
    ('Fried Rice',           180),
    ('Dal Makhani',          220),
    ('Butter Naan x4',       160),
]
PAYMENTS    = ['UPI','Credit Card','Debit Card',
               'Cash on Delivery','ZIPIT Wallet']
STATUS_FLOW = ['placed','confirmed','preparing',
               'out_for_delivery','delivered']

print('ZIPIT Live Order Producer started')
print(f'Writing to: s3://{BUCKET}/raw/orders/')
print('Press Ctrl+C to stop\n')

count = 0
while True:
    order_id   = f'ORD{200000 + count}'
    item,price = random.choice(FOODS)
    qty        = random.randint(1, 3)
    city       = random.choice(CITIES)
    total      = max(0,
                   price * qty
                   + random.choice([0, 20, 30, 49])
                   - random.choice([0, 0, 0, 20, 50]))

    print(f'Order #{count+1}: {order_id} | {city}')
    print(f'  {item} x{qty} = Rs.{total}')

    # Build full order event with all status updates
    events = []
    placed_at = datetime.utcnow()
    for i, status in enumerate(STATUS_FLOW):
        event = {
            'event_id'     : str(uuid.uuid4()),
            'order_id'     : order_id,
            'customer_id'  : f'CUST{random.randint(1,500):05d}',
            'restaurant_id': f'REST{random.randint(1,100):04d}',
            'rider_id'     : f'RIDER{random.randint(1,200):04d}'
                             if status in
                             ['out_for_delivery','delivered']
                             else '',
            'item_name'    : item,
            'quantity'     : qty,
            'item_price'   : price,
            'total_amount' : total,
            'payment_method':random.choice(PAYMENTS),
            'city'         : city,
            'status'       : status,
            'timestamp'    : datetime.utcnow().isoformat(),
        }
        events.append(event)
        print(f'  → {status}')

    # Write this order's events as one JSON file to S3
    timestamp  = datetime.utcnow().strftime(
                   '%Y%m%d_%H%M%S_%f')
    s3_key     = f'raw/orders/live/{order_id}_{timestamp}.json'

    s3.put_object(
        Bucket      = BUCKET,
        Key         = s3_key,
        Body        = json.dumps(events, indent=2),
        ContentType = 'application/json'
    )
    print(f'  Saved to S3: {s3_key}')
    print()

    count += 1
    # Wait 3 seconds before next order
    # This simulates real-time streaming
    time.sleep(3)