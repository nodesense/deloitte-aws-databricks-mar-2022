import boto3
import random
import uuid
import datetime
import json
import time

kinesis_stream_name = 'invoice-stream'

SAMPLES = 10000
DELAY = 5 # seconds

countries = ["USA", "CA", "IN", "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]
stock_codes = ['85123A', '71053', '84406B', '84406G', '84406E']
customer_codes = [17850, 13047, 12583, 17850]

kinesis_client = boto3.client('kinesis', region_name='us-east-2')

for i in range(SAMPLES):
    invoice_no = str(uuid.uuid4().fields[-1])[:6]
    invoice_no = int(invoice_no)
    
    customer_code =  random.choice(customer_codes)
    country =  random.choice(countries)
    
    #customer = random.choice(customers)
    #customer_code = customer["CustomerID"]
    #country = customer["Country"]
    
    current_time = datetime.datetime.now()
 
    invoice_date = current_time.strftime('%m/%d/%Y %H:%M') 
    
    number_of_items = random.randint(3, 10)
    
    for j in range(number_of_items):
        # MM/dd/yyyy hh:mm
        quantity = random.randint(1, 10)
        unit_price = float(random.randint(1, 5))
        stock_code =  random.choice(stock_codes)
        invoice = {  "InvoiceNo": invoice_no,
                     "StockCode": stock_code ,
                     "Quantity": quantity,
                    "Description": "TODO",
                    "InvoiceDate": invoice_date,
                    "UnitPrice": unit_price,
                    "CustomerID": customer_code,
                     "Country"  : country   }
    
        invoice_str = json.dumps(invoice)
        print ("POS ", invoice_str)

        key = invoice["Country"]
         
        put_response = kinesis_client.put_record(
                        StreamName=kinesis_stream_name,
                        Data=invoice_str,
                        PartitionKey=invoice["Country"])
        print("response ", put_response)
        
        
    time.sleep(DELAY)