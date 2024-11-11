# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in gangireddy_reddy_databricks_npmentorskool_onmicrosoft_com.silver_ecom1;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gangireddy_reddy_databricks_npmentorskool_onmicrosoft_com;
# MAGIC use schema silver_ecom1;

# COMMAND ----------

# Load tables
orders = spark.table("orders")
order_items = spark.table("order_items")
returns = spark.table("returns")
products = spark.table("products")
addresses = spark.table("addresses")
suppliers = spark.table("suppliers")
customers = spark.table("customers")
payment_methods = spark.table("payment_methods")
payments = spark.table("payments")
shipping_tier = spark.table("shipping_tier")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database gangireddy_reddy_databricks_npmentorskool_onmicrosoft_com.gold_ecom;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer_Dimension Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_ecom.customer_dimension (
# MAGIC     customer_sk_key STRING,
# MAGIC     Customer_ID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Email STRING,
# MAGIC     PhoneNumber LONG,
# MAGIC     DateOfBirth DATE,
# MAGIC     RegistrationDate DATE
# MAGIC ) USING DELTA

# COMMAND ----------

customers_silver_df = spark.table("customers")

# Add surrogate key (UUID)
customers_silver_df = customers_silver_df.withColumn("customer_sk_key", expr("uuid()"))

# COMMAND ----------

# Perform the merge into operation
customers_silver_df.createOrReplaceTempView("customer_silver_temp")

# SQL query to merge the data from the silver layer into the gold dimension table
spark.sql("""
MERGE INTO gold_ecom.customer_dimension AS target
USING customer_silver_temp AS source
ON target.Customer_ID = source.CustomerID
WHEN MATCHED THEN
  UPDATE SET
    target.FirstName = source.FirstName,
    target.LastName = source.LastName,
    target.Email = source.Email,
    target.PhoneNumber = source.PhoneNumber,
    target.DateOfBirth = source.DateOfBirth,
    target.RegistrationDate = source.RegistrationDate
WHEN NOT MATCHED THEN
  INSERT (customer_sk_key, Customer_ID, FirstName, LastName, Email, PhoneNumber, DateOfBirth, RegistrationDate)
  VALUES (source.customer_sk_key, source.CustomerID, source.FirstName, source.LastName, source.Email, source.PhoneNumber, source.DateOfBirth, source.RegistrationDate)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Payments_Dimensions Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_ecom.payments_dimension (
# MAGIC     payment_sk_key STRING PRIMARY KEY,
# MAGIC     PaymentID STRING,
# MAGIC     OrderID STRING,
# MAGIC     PaymentDate TIMESTAMP,
# MAGIC     PaymentMethodID STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

payments_silver_df = spark.table("payments") 

# Step 1: Add surrogate key (UUID) for the Payments dimension
payments_silver_df = payments_silver_df.withColumn("payment_sk_key", expr("uuid()"))

# Step 2: Merge data into the Payments_Dimension table in the Gold layer
payments_silver_df.createOrReplaceTempView("payments_silver_temp")

spark.sql("""
MERGE INTO gold_ecom.payments_dimension AS target
USING payments_silver_temp AS source
ON target.PaymentID = source.PaymentID
WHEN MATCHED THEN
  UPDATE SET
    target.OrderID = source.OrderID,
    target.PaymentDate = source.PaymentDate,
    target.PaymentMethodID = source.PaymentMethodID
WHEN NOT MATCHED THEN
  INSERT (payment_sk_key, PaymentID, OrderID, PaymentDate, PaymentMethodID)
  VALUES (source.payment_sk_key, source.PaymentID, source.OrderID, source.PaymentDate, source.PaymentMethodID)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders_Dimesions Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_ecom.orders_dimension (
# MAGIC     order_sk_key STRING,
# MAGIC     OrderID STRING,
# MAGIC     CustomerID STRING,
# MAGIC     OrderDate TIMESTAMP,
# MAGIC     ShippingTierID STRING,
# MAGIC     SupplierID STRING,
# MAGIC     OrderChannel STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# Load the silver layer data
orders_silver_df = spark.table("silver_ecom1.orders")

# Add a surrogate key (UUID)
orders_silver_df = orders_silver_df.withColumn("order_sk_key", expr("uuid()"))

# Create a temporary view for SQL operations
orders_silver_df.createOrReplaceTempView("orders_silver_temp")

# COMMAND ----------

# Perform the merge operation
spark.sql("""
MERGE INTO gold_ecom.orders_dimension AS target
USING orders_silver_temp AS source
ON target.OrderID = source.OrderID
WHEN MATCHED THEN
  UPDATE SET
    target.CustomerID = source.CustomerID,
    target.OrderDate = source.OrderDate,
    target.ShippingTierID = source.ShippingTierID,
    target.SupplierID = source.SupplierID,
    target.OrderChannel = source.OrderChannel
WHEN NOT MATCHED THEN
  INSERT (order_sk_key, OrderID, CustomerID, OrderDate, ShippingTierID, SupplierID, OrderChannel)
  VALUES (source.order_sk_key, source.OrderID, source.CustomerID, source.OrderDate, source.ShippingTierID, source.SupplierID, source.OrderChannel)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products_Dimesions Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_ecom.products_dimension (
# MAGIC     product_sk_key STRING PRIMARY KEY,
# MAGIC     Product_ID STRING,
# MAGIC     Product_Name STRING,
# MAGIC     Product_Category STRING,
# MAGIC     Product_Sub_Category STRING
# MAGIC );

# COMMAND ----------

# Reading the silver layer table into a DataFrame
products_silver_df = spark.table("silver_ecom1.products")

# Adding a surrogate key (UUID)
products_silver_df = products_silver_df.withColumn("product_sk_key", expr("uuid()"))

# COMMAND ----------

# Creating a temporary view for the products data
products_silver_df.createOrReplaceTempView("products_silver_temp")

# COMMAND ----------

# Applying the MERGE INTO operation
spark.sql("""
MERGE INTO gold_ecom.products_dimension AS target
USING products_silver_temp AS source
ON target.Product_ID = source.Product_ID
WHEN MATCHED THEN
  UPDATE SET
    target.Product_Name = source.Product_Name,
    target.Product_Category = source.Product_Category,
    target.Product_Sub_Category = source.Product_Sub_Category
WHEN NOT MATCHED THEN
  INSERT (product_sk_key, Product_ID, Product_Name, Product_Category, Product_Sub_Category)
  VALUES (source.product_sk_key, source.Product_ID, source.Product_Name, source.Product_Category, source.Product_Sub_Category)
""")

# COMMAND ----------

products_silver_df = spark.table("products")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders_Items_Dimensions Table

# COMMAND ----------

# Load the order_items data from the silver layer
order_items_df = spark.table("silver_ecom1.order_items")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_ecom.order_items_dimension (
# MAGIC     order_items_sk_key STRING PRIMARY KEY,
# MAGIC     OrderItemID STRING,
# MAGIC     OrderID STRING,
# MAGIC     ProductID STRING,
# MAGIC     Quantity_purchased Integer
# MAGIC );

# COMMAND ----------

# Reading the silver layer table into a DataFrame
order_items_silver_df = spark.table("silver_ecom1.order_items")

order_items_silver_df = order_items_silver_df.withColumn("order_items_sk_key", expr("uuid()"))

# COMMAND ----------

# Creating a temporary view for the order items data
order_items_silver_df.createOrReplaceTempView("order_items_silver_temp")

# Applying the MERGE INTO operation
spark.sql("""
MERGE INTO gold_ecom.order_items_dimension AS target
USING order_items_silver_temp AS source
ON target.OrderItemID = source.OrderItemID
WHEN MATCHED THEN
  UPDATE SET
    target.OrderID = source.OrderID,
    target.ProductID = source.ProductID
WHEN NOT MATCHED THEN
  INSERT (order_items_sk_key, OrderItemID, OrderID, ProductID)
  VALUES (source.order_items_sk_key, source.OrderItemID, source.OrderID, source.ProductID)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Addresses_Dimesions Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_ecom.addresses_dimension (
# MAGIC     addresses_sk_key STRING,
# MAGIC     AddressID STRING,
# MAGIC     CustomerID STRING,
# MAGIC     AddressLine1 STRING,
# MAGIC     City STRING,
# MAGIC     State STRING,
# MAGIC     PinCode FLOAT,
# MAGIC     AddressType STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

addresses_silver_df = spark.table("silver_ecom1.addresses")
addresses_silver_df = addresses_silver_df.withColumn("addresses_sk_key", expr("uuid()"))
addresses_silver_df.createOrReplaceTempView("addresses_silver_temp")

# COMMAND ----------

spark.sql("""
MERGE INTO gold_ecom.addresses_dimension AS target
USING addresses_silver_temp AS source
ON target.AddressID = source.AddressID
WHEN MATCHED THEN
    UPDATE SET
        target.CustomerID = source.CustomerID,
        target.AddressLine1 = source.AddressLine1,
        target.City = source.City,
        target.State = source.State,
        target.PinCode = source.PinCode,
        target.AddressType = source.AddressType
WHEN NOT MATCHED THEN
    INSERT (addresses_sk_key, AddressID, CustomerID, AddressLine1, City, State, PinCode, AddressType)
    VALUES (
        source.addresses_sk_key, 
        source.AddressID, 
        source.CustomerID, 
        source.AddressLine1, 
        source.City, 
        source.State, 
        source.PinCode, 
        source.AddressType
    )
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Return_Dimesions Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_ecom.returns_dimension (
# MAGIC   returns_sk_key STRING,
# MAGIC   OrderID STRING,
# MAGIC   Return_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# Reading the silver layer data
returns_silver_df = spark.table("silver_ecom1.returns")

# Adding a surrogate key
returns_silver_df = returns_silver_df.withColumn("returns_sk_key", expr("uuid()"))

# Creating or replacing a temporary view
returns_silver_df.createOrReplaceTempView("returns_silver_temp")


# COMMAND ----------

# Merging data into the gold layer table
spark.sql("""
MERGE INTO gold_ecom.returns_dimension AS target
USING returns_silver_temp AS source
ON target.OrderID = source.OrderID
WHEN MATCHED THEN
  UPDATE SET 
    target.Return_reason = source.Return_reason
WHEN NOT MATCHED THEN
  INSERT (returns_sk_key, OrderID, Return_reason)
  VALUES (source.returns_sk_key, source.OrderID, source.Return_reason)
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Time_Dimension Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_ecom.time_dimension (
# MAGIC   time_sk_key STRING,
# MAGIC   Time_Id STRING,
# MAGIC   Date DATE,
# MAGIC   Day_of_week STRING,
# MAGIC   Day_of_month INT,
# MAGIC   Month STRING,
# MAGIC   Month_num INT,
# MAGIC   Quarter STRING,
# MAGIC   Quarter_Num INT,
# MAGIC   Year INT,
# MAGIC   Is_holiday BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in gold_ecom;

# COMMAND ----------

# Load the data from the silver layer
order_items_df = spark.table("silver_ecom1.order_items")
products_df = spark.table("silver_ecom1.products")

# COMMAND ----------

# Load necessary dimension tables
orders_df = spark.table("gold_ecom.orders_dimension")
customers_df = spark.table("gold_ecom.customer_dimension")
payments_df = spark.table("gold_ecom.payments_dimension")
addresses_df = spark.table("gold_ecom.addresses_dimension")

# COMMAND ----------

# Join the tables to create the fact table
fact_sales_df = orders_df.alias("o") \
    .join(order_items_df.alias("oi"), col("o.OrderID") == col("oi.OrderID"), "left") \
    .join(products_df.alias("p"), col("oi.ProductID") == col("p.Product_ID"), "left") \
    .join(customers_df.alias("c"), col("o.CustomerID") == col("c.Customer_ID"), "left") \
    .join(payments_df.alias("pay"), col("o.OrderID") == col("pay.OrderID"), "left") \
    .join(addresses_df.alias("a"), col("o.CustomerID") == col("a.CustomerID"), "left") \
    .select(
        monotonically_increasing_id().alias("fact_sales_sk_key"),
        col("pay.PaymentID").alias("Payment_ID"),
        col("c.Customer_ID").alias("Customer_ID"),
        col("p.Product_ID").alias("Product_ID"),
        col("o.OrderID").alias("Order_ID"),
        col("a.AddressID").alias("Address_ID"),
        (col("oi.Quantity") * col("p.Discounted_Price")).alias("Sales_amount"),
        col("oi.Quantity").alias("Quantity_purchased"),
        col("p.Actual_Price").alias("Actual_price"),  
        col("p.Discounted_Price").alias("Discounted_price") 
    )

# COMMAND ----------

# Write the fact_sales DataFrame to the gold layer
fact_sales_df.write.format("delta").mode("overwrite").saveAsTable("gold_ecom.fact_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_ecom.vw_total_sales_by_city AS
# MAGIC select a.City, sum(f.Sales_amount) as sales
# MAGIC from gold_ecom.fact_sales f
# MAGIC join gold_ecom.addresses_dimension a
# MAGIC on f.Address_ID = a.AddressID
# MAGIC group by a.City

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_ecom.vw_total_sales_by_city
# MAGIC where sales is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view gold_ecom.vw_total_sales_by_payment_method as
# MAGIC select p.PaymentMethodID, sum(f.Sales_amount) as sales
# MAGIC from gold_ecom.fact_sales f
# MAGIC join gold_ecom.payments_dimension p
# MAGIC on f.Payment_ID = p.PaymentID
# MAGIC group by p.PaymentMethodID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_ecom.vw_total_sales_by_payment_method order by sales desc;

# COMMAND ----------


