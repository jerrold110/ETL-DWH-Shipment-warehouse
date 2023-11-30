def transform_load(job_timestamp):
	"""
	To ensure scalibility dataframes are partitioned before major operations, cached if used repeatedly, SQL is used for complex transformations as the SQL optimizer may improve performance.
	Further improvements may be yielded by optimising data types
	"""
	import os

	# Create the destination folder structure
	#isExist = os.path.exists('./transformed_data')
	#if not isExist:
		#print('Creating transformed_data folder')
		#os.makedirs('./transformed_data')
		
	isExist = os.path.exists(f'./transformed_data/{job_timestamp}')
	if not isExist:
		os.makedirs(f'./transformed_data/{job_timestamp}')

	# Create the spark context
	# from pyspark import SparkContext
	
	# sc = SparkContext("local", "MyApp")
	
	# Create the SparkSession
	from pyspark.sql import SparkSession
	spark = SparkSession.builder.master("local[*]") \
		                .appName('Shipment data ETL') \
		                .getOrCreate()
	from pyspark.sql.functions import lit
                
	df_customers = spark.read.csv(f'extracted_data/{job_timestamp}/Customers.csv', header=True, inferSchema=True).repartition(10)
	df_shipments = spark.read.csv(f'extracted_data/{job_timestamp}/Shipments.csv', header=True, inferSchema=True).repartition(10)
	df_shipments = df_shipments.withColumnRenamed("Reached.on.Time_Y.N","Reached_ontime")
	
	#df_customers.printSchema()
	#df_shipments.printSchema()
	#print((df_shipments.count(), len(df_shipments.columns)))
	#print((df_customers.count(), len(df_customers.columns)))
	
	# Data quality checks
	"""

	"""

	# cleaning operation on shipments
	df_shipments = df_shipments.filter(df_shipments.Cost_of_the_Product>0)
	# cleaning operations on customers
	df_customers = df_customers.filter((df_customers.Age>0) & (df_customers.Age<99))
	df_customers = df_customers.filter(df_customers.Gender.isin(['M', 'F']))
	# cache dataframes for repeated use
	df_shipments.cache()
	df_customers.cache()

	df_shipments.createOrReplaceTempView('shipments')
	df_customers.createOrReplaceTempView('customers')

	# WarehouseBlockDim table
	print(df_shipments.show(1))

	WarehouseBlockDim = spark.sql(
		"""
		select
		Warehouse_block as WarehouseBlock,
		sum
		(
		case 
		    when Reached_ontime = 1 then 1
		    else 0
		end
		) as TotalEarlyShipments,
		sum
		(
		case 
		    when Reached_ontime = 0 then 1
		    else 0
		end
		) as TotalLateShipments
		from shipments
		group by Warehouse_block
		"""
	)
	WarehouseBlockDim.show()
	#cwd = os.getcwd()
	#from datetime import datetime
	#timestamp_str_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
	#WarehouseBlockDim.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=1', header=True)

	# ShipmentEmbarkMonth table
	ShipmentEmbarkMonth = spark.sql(
		"""
		select
		year(Delivery_start) as Year,
		month(Delivery_start) as Month,
		sum
		(
		case
		    when Mode_of_Shipment = 'Flight' then 1
		    else 0
		end
		) as TotalFlightShipments,
		sum
		(
		case
		    when Mode_of_Shipment = 'Ship' then 1
		    else 0
		end
		) as TotalShipShipments,
		sum
		(
		case
		    when Mode_of_Shipment = 'Road' then 1
		    else 0
		end
		) as TotalRoadShipments
		
		from shipments
		group by Year, Month
		"""
	)
	ShipmentEmbarkMonth.show()

	shipment_customer = spark.sql(
		"""
		select 
		s.*,
		c.age
		from shipments as s left join customers as c
		on s.Customer_id = c.id
		"""
	)
	shipment_customer.show(1)
	shipment_customer.cache()
	shipment_customer.createOrReplaceTempView('shipment_customer')

	# ShipmentModeDim
	ShipmentModeDim = spark.sql(
		"""
		select
		Mode_of_Shipment as ModeOfShipment,
		sum
		(
		case 
		    when Reached_ontime = 1 then 1
		    else 0
		end
		) as TotalEarlyShipments,
		sum
		(
		case 
		    when Reached_ontime = 0 then 1
		    else 0
		end
		) as TotalLateShipments
		from shipment_customer
		group by ModeOfShipment
		"""
	)
	ShipmentModeDim.show(1)

	#CustomerDim
	CustomerDim = spark.sql(
		"""
		select
		Customer_id as CustomerID,
		Gender,
		age as Age,
		avg(Customer_care_calls) as AvgCalls,
		avg(Customer_rating) as AvgRaRating,
		count(*) as TotalPurchases
		from shipment_customer
		group by CustomerID, Gender, age
		"""
	)
	CustomerDim.show(1)

	# Shipment table
	Shipment = spark.sql(
		"""
		select 
		ShipmentId,
		customer_id as CustomerID,
		warehouse_block as WarehouseBlock,
		mode_of_shipment as ModeOfShipment,
		customer_care_calls as CustomerCareCalls,
		Customer_rating as CustomerRating,
		cost_of_the_product as Cost,
		product_importance as ProductImportance,
		discount_offered as DiscountOffered,
		weight_in_gms as WeightInGrams,
		delivery_start as ShipmentStartDate,
		delivery_end as ShipmentEndDate,
		reached_ontime as ReachedOnTime
		from shipment_customer
		"""
	)
	Shipment.show(1)
	
	# add timestamps as new columns for idempotency
	def add_timestamp(df):
		return df.withColumn("batch_timestamp", lit(job_timestamp))
		
	WarehouseBlockDim = add_timestamp(WarehouseBlockDim)
	ShipmentEmbarkMonth = add_timestamp(ShipmentEmbarkMonth)
	ShipmentModeDim = add_timestamp(ShipmentModeDim)
	CustomerDim = add_timestamp(CustomerDim)
	Shipment = add_timestamp(Shipment)
	WarehouseBlockDim.show(1)
	
	print('transformation complete')
	
	# Repartition the dataframes into 10 partitions
	WarehouseBlockDim.repartition(10)
	ShipmentEmbarkMonth.repartition(10)
	ShipmentModeDim.repartition(10)
	CustomerDim.repartition(10)
	Shipment.repartition(10)
	
	#Export each dataframe as snapshop
	cwd = os.getcwd()
	WarehouseBlockDim.write.parquet(f'{cwd}/transformed_data/{job_timestamp}/key=1')
	ShipmentEmbarkMonth.write.parquet(f'{cwd}/transformed_data/{job_timestamp}/key=2')
	ShipmentModeDim.write.parquet(f'{cwd}/transformed_data/{job_timestamp}/key=3',)
	CustomerDim.write.parquet(f'{cwd}/transformed_data/{job_timestamp}/key=4')
	Shipment.write.parquet(f'{cwd}/transformed_data/{job_timestamp}/key=5')
	print('data transformation snapshot complete')
	
	# Load the data to redshift
	import redshift_connector
	def load_data(table_name, df):
		conn = redshift_connector.connect(
		host='my_redshift_cluster.538801499856.us-east-1.redshift.amazonaws.com',
		database='dev',
		port=5439,
		user='admin',
		password='my_password')
			
		cursor = conn.cursor()
		cursor.execute("""
		
		""")
	
	
	
	
if __name__ == '__main__':
	from datetime import datetime
	transform_load(datetime(2011, 3, 29, 0, 0,0))
