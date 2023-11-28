def transform():

	from pyspark import SparkContext
	sc = SparkContext("local", "MyApp")

	# Create the SparkSession
	from pyspark.sql import SparkSession

	spark = SparkSession.builder.master("local[1]") \
		                .appName('Shipment data ETL') \
		                .getOrCreate()
	# Create the destination folder
	isExist = os.path.exists('./transformed_data')
	if not isExist:
		print('Creating transformed_data folder')
		os.makedirs('./transformed_data_')

	print('pyspark script finished')
