import boto3
import os

def extract(job_timestamp):
	"""
	Ensure that Aws cli configuration with IAM is already done to resolve authentication issues
	"""
	# Create the destination folder
	isExist = os.path.exists('./extracted_data')
	if not isExist:
		print('Creating extracted_data folder')
		os.makedirs('./extracted_data')
	
	isExist = os.path.exists(f'./extracted_data/{job_timestamp}')
	if not isExist:
		os.makedirs(f'./extracted_data/{job_timestamp}')

	# Downloads Customers.csv to path relative to the current working directory
	s3 = boto3.resource('s3')
	
	s3.Object(bucket_name='test-project-j2400-2', key='Shipments.csv')\
	.download_file(f'./extracted_data/{job_timestamp}/Shipments.csv')
	
	s3.Object(bucket_name='test-project-j2400-2', key='Customers.csv')\
	.download_file(f'./extracted_data/{job_timestamp}/Customers.csv')

	print('Python extract done')

if __name__ == '__main__':
	from datetime import datetime
	extract(datetime(2011, 3, 29, 0, 0,0))
