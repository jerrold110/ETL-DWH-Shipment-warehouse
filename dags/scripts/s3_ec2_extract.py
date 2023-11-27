import boto3
s3 = boto3.resource('s3')
	
# trying to download Customers.csv
s = s3

# Downloads Customers.csv to path relative to the current working directory
s.Object(bucket_name='test-project-j2400-2', key='Customers.csv')\
.download_file('../extracted_data/Customers.csv')

s.Object(bucket_name='test-project-j2400-2', key='Shipments.csv')\
.download_file('../extracted_data/Shipments.csv')

# Data quality checks
"""

"""

print('Extract py script done')
