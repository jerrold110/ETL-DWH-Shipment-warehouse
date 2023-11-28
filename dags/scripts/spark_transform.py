def transform():
	"""
	To ensure scalibility dataframes are partitioned before major operations, cached if used repeatedly, SQL is used for complex transformations as the SQL optimizer may improve performance.
	Further improvements may be yielded by optimising data types, and working with parquet files and further parallel processing.
	"""

	import os

	# Create the destination folder
	isExist = os.path.exists('./transformed_data')
	if not isExist:
		print('Creating transformed_data folder')
		os.makedirs('./transformed_data')

	# Create the spark context
	from pyspark import SparkContext
	sc = SparkContext("local", "MyApp")
	# Create the SparkSession
	from pyspark.sql import SparkSession
	spark = SparkSession.builder.master("local[*]") \
		                .appName('Shipment data ETL') \
		                .getOrCreate()
		                
	df_customers = spark.read.csv('extracted_data/Customers.csv', header=True, inferSchema=True).repartition(10)
	df_shipments = spark.read.csv('extracted_data/Shipments.csv', header=True, inferSchema=True).repartition(10)
	df_shipments = df_shipments.withColumnRenamed("Reached.on.Time_Y.N","Reached_ontime")
	#df_customers.printSchema()
	#df_shipments.printSchema()

	#print((df_shipments.count(), len(df_shipments.columns)))
	#print((df_customers.count(), len(df_customers.columns)))

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
	cwd = os.getcwd()
	from datetime import datetime
	timestamp_str_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
	WarehouseBlockDim.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=1', header=True)

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
	ShipmentModeDim.show()

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
	CustomerDim.show(10)

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
	Shipment.show(10)
	Shipment.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=5', header=True)
	print('transformation complete')
	
	# Export each dataframes as multiple csv files to /transformed_data
	# Repartition the dataframes into 10 partitions
	WarehouseBlockDim.repartition(10)
	ShipmentEmbarkMonth.repartition(10)
	ShipmentModeDim.repartition(10)
	CustomerDim.repartition(10)
	Shipment.repartition(10)
	
	cwd = os.getcwd()
	from datetime import datetime
	timestamp_str_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
	#WarehouseBlockDim.repartition(10).write.csv(f'{cwd}/transformed_data', header=True, mode='overwrite')
	WarehouseBlockDim.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=1', header=True)
	ShipmentEmbarkMonth.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=2', header=True)
	ShipmentModeDim.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=3', header=True)
	CustomerDim.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=4', header=True)
	Shipment.write.csv(f'{cwd}/transformed_data/{timestamp_str_utc}/key=5', header=True)
	print('data transformation snapshot write complete')
	
	# Load these dataframes into the data warehouse via jdbc
	"""
	df.write \
  .format("jdbc") \
  .option("url", "jdbc:redshift://your-redshift-cluster:5439/your_database") \
  .option("dbtable", "your_table") \
  .option("user", "your_username") \
  .option("password", "your_password") \
  .option("numPartitions", 10)  # Adjust the number of partitions as needed
  .mode("append") \
  .save()
	"""
	isExist = os.path.exists('./transformed_data_done')
	if not isExist:
		os.makedirs('./transformed_data_done')


	
if __name__ == '__main__':
	transform()
