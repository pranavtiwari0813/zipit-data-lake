# ingestion/batch_upload.py
# Uploads any CSV file to the correct S3 raw/ zone
# Use this for daily historical data uploads

import boto3, os, sys
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

s3     = boto3.client('s3',
           region_name=os.getenv('ap-south-1'))
BUCKET = os.getenv('zipit-datalake-engineer')

# Auto-detect which S3 folder based on filename
FOLDER_MAP = {
    'order'      : 'orders',
    'customer'   : 'customers',
    'restaurant' : 'restaurants',
    'rider'      : 'riders',
    'payment'    : 'payments',
}

def detect_folder(filename):
    lower = filename.lower()
    for keyword, folder in FOLDER_MAP.items():
        if keyword in lower:
            return folder
    return 'misc'

def upload_file(local_path):
    if not os.path.exists(local_path):
        print(f'File not found: {local_path}')
        return
    filename  = os.path.basename(local_path)
    folder    = detect_folder(filename)
    timestamp = datetime.utcnow().strftime(
                  '%Y%m%d_%H%M%S')
    s3_key    = f'raw/{folder}/{timestamp}_{filename}'
    size_kb   = os.path.getsize(local_path) / 1024

    print(f'Uploading: {filename}')
    print(f'  Size   : {size_kb:.1f} KB')
    print(f'  To     : s3://{BUCKET}/{s3_key}')

    s3.upload_file(local_path, BUCKET, s3_key)
    print(f'  Done   : Upload complete')
    return s3_key

if __name__ == '__main__':
    if len(sys.argv) > 1:
        upload_file(sys.argv[1])
    else:
        # Upload all CSVs in sample_data folder
        print('Uploading all sample_data/ files...\n')
        folder = 'sample_data'
        for f in os.listdir(folder):
            if f.endswith('.csv'):
                upload_file(
                  os.path.join(folder, f))
                print()
        print('All files uploaded to S3!')