import boto3
import os

def extract():
	"""
	Ensure that Aws cli configuration with IAM is already done to resolve authentication issues
	"""
	# Create the destination folder
	isExist = os.path.exists('./extracted_data')
	if not isExist:
		print('Creating extracted_data folder')
		os.makedirs('./extracted_data')
	
	s3 = boto3.resource('s3')

	# Downloads Customers.csv to path relative to the current working directory
	s3.Object(bucket_name='test-project-j2400-2', key='Customers.csv')\
	.download_file('./extracted_data/Customers.csv')

	s3.Object(bucket_name='test-project-j2400-2', key='Shipments.csv')\
	.download_file('./extracted_data/Shipments.csv')
	
	print('Python extract done')

	# Data quality checks
	"""

	"""

if __name__ == '__main__':
	extract()
