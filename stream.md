```
{"InvoiceNo": 225124, "StockCode": "85123A", "Quantity": 10, 
"Description": "TODO", "InvoiceDate": "03/25/2022 15:13", 
"UnitPrice": 2.0, "CustomerID": 12583, "Country": "BE"}
```

# read files as stream

```
checkpoint_path = '/tmp/delta-gks/invoices/_checkpoints'

# in the place of gks, use your s3 mount
# where output goes, delta files in parquet format, _delta_logs/*.json
write_path = '/mnt/gks/delta-gks/invoices'

# Input data
upload_path = "s3://trainingmar22-invoices/invoices/"
```

# anaylytics

# delta format

```
```
