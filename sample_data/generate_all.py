import boto3, csv, random, uuid, os, io
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv

# Load your .env file
load_dotenv()

fake   = Faker('en_IN')  # Indian names and addresses
s3     = boto3.client('s3',
           region_name=os.getenv('AWS_REGION'))
BUCKET = os.getenv('ZIPIT_BUCKET')

# ── Master data for ZIPIT ─────────────────────────────────
CITIES   = ['Delhi','Mumbai','Bangalore','Pune',
            'Hyderabad','Chennai','Kolkata']
CUISINES = ['North Indian','South Indian','Chinese',
            'Pizza','Biryani','Burger','Rolls']
FOODS    = [
    ('Chicken Biryani',320),
    ('Paneer Butter Masala',280),
    ('Veg Pizza',250),
    ('Masala Dosa',120),
    ('Chicken Burger',150),
    ('Fried Rice',180),
    ('Dal Makhani',220),
    ('Butter Naan x4',160),
    ('Hakka Noodles',170),
    ('Cold Coffee',110),
]
PAYMENTS  = ['UPI','Credit Card','Debit Card',
             'Cash on Delivery','ZIPIT Wallet']
STATUSES  = ['placed','confirmed','preparing',
             'out_for_delivery','delivered','cancelled']
STATUS_W  = [5, 5, 10, 15, 60, 5]
GATEWAYS  = ['Razorpay','PayU','Paytm','Cashfree']
base      = datetime(2025, 1, 1)

# ── Helper: upload CSV to S3 ──────────────────────────────
def upload(rows, filename, prefix):
    buf = io.StringIO()
    w   = csv.DictWriter(buf,
                         fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)
    s3.put_object(
        Bucket = BUCKET,
        Key    = f'raw/{prefix}/{filename}',
        Body   = buf.getvalue()
    )
    print(f'  Done: {filename} — {len(rows):,} rows')

# ── 1. Restaurants (100) ──────────────────────────────────
print('Creating restaurants...')
rests = []
for i in range(100):
    city = random.choice(CITIES)
    rests.append({
        'restaurant_id': f'REST{i+1:04d}',
        'name'         : fake.company() + ' Restaurant',
        'city'         : city,
        'cuisine'      : random.choice(CUISINES),
        'rating'       : round(random.uniform(3.2,4.9),1),
        'avg_prep_mins': random.randint(15,45),
        'is_active'    : random.choice(
                           [True,True,True,False]),
        'joined_date'  : (base - timedelta(
                           days=random.randint(30,730))
                         ).date().isoformat(),
    })
upload(rests, 'restaurants.csv', 'restaurants')

# ── 2. Customers (500) ────────────────────────────────────
print('Creating customers...')
custs = []
for i in range(500):
    custs.append({
        'customer_id'      : f'CUST{i+1:05d}',
        'name'             : fake.name(),
        'phone'            : f'9{random.randint(100000000,999999999)}',
        'city'             : random.choice(CITIES),
        'signup_date'      : (base - timedelta(
                               days=random.randint(1,365))
                             ).date().isoformat(),
        'total_orders'     : random.randint(1, 200),
        'loyalty_points'   : random.randint(0, 5000),
        'preferred_cuisine': random.choice(CUISINES),
    })
upload(custs, 'customers.csv', 'customers')

# ── 3. Riders (200) ───────────────────────────────────────
print('Creating riders...')
riders = []
for i in range(200):
    d = random.randint(10, 3000)
    riders.append({
        'rider_id'        : f'RIDER{i+1:04d}',
        'name'            : fake.name(),
        'city'            : random.choice(CITIES),
        'vehicle_type'    : random.choice(
                              ['Bike','Scooter','Cycle']),
        'rating'          : round(random.uniform(3.5,5.0),1),
        'total_deliveries': d,
        'avg_delivery_mins':round(random.uniform(22,48),1),
        'is_active'       : random.choice([True,True,False]),
    })
upload(riders, 'riders.csv', 'riders')

# ── 4. Orders (5000) ─────────────────────────────────────
print('Creating orders...')
orders = []
for i in range(5000):
    placed = base + timedelta(
        days   = random.randint(0, 89),
        hours  = random.randint(6, 23),
        minutes= random.randint(0, 59))
    item, price = random.choice(FOODS)
    qty    = random.randint(1, 3)
    total  = max(0,
               price * qty
               + random.choice([0, 0, 20, 30, 49])
               - random.choice([0, 0, 0, 20, 50]))
    status = random.choices(
               STATUSES, weights=STATUS_W)[0]
    orders.append({
        'order_id'       : f'ORD{100000 + i}',
        'customer_id'    : f'CUST{random.randint(1,500):05d}',
        'restaurant_id'  : f'REST{random.randint(1,100):04d}',
        'rider_id'       : f'RIDER{random.randint(1,200):04d}'
                           if status not in
                           ['placed','confirmed','cancelled']
                           else '',
        'item_name'      : item,
        'quantity'       : qty,
        'item_price'     : price,
        'total_amount'   : total,
        'payment_method' : random.choice(PAYMENTS),
        'city'           : random.choice(CITIES),
        'status'         : status,
        'placed_at'      : placed.isoformat(),
        'delivery_mins'  : random.randint(22, 72)
                           if status == 'delivered' else '',
        'customer_rating': random.randint(1, 5)
                           if status == 'delivered'
                           and random.random() > 0.3
                           else '',
    })
upload(orders, 'orders.csv', 'orders')

# ── 5. Payments ───────────────────────────────────────────
print('Creating payments...')
pays = []
for o in orders:
    if o['status'] != 'cancelled':
        pays.append({
            'payment_id'  : f'PAY{uuid.uuid4().hex[:10].upper()}',
            'order_id'    : o['order_id'],
            'customer_id' : o['customer_id'],
            'amount'      : o['total_amount'],
            'method'      : o['payment_method'],
            'status'      : random.choices(
                              ['success','failed','refunded'],
                              weights=[93, 4, 3])[0],
            'gateway'     : random.choice(GATEWAYS),
            'time'        : o['placed_at'],
            'city'        : o['city'],
        })
upload(pays, 'payments.csv', 'payments')

# ── Done ──────────────────────────────────────────────────
print()
print('ALL ZIPIT DATA UPLOADED TO S3!')
print(f'  Restaurants : 100 rows')
print(f'  Customers   : 500 rows')
print(f'  Riders      : 200 rows')
print(f'  Orders      : 5,000 rows')
print(f'  Payments    : {len(pays):,} rows')
print()
print(f'Check: https://s3.console.aws.amazon.com/s3/buckets/{BUCKET}')