import os
from pyspark.sql import SparkSession

# Configuração do Spark para acessar o S3
spark = (
    SparkSession.builder.appName("Medallion Architecture")
    .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.508.jar")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY_ID")
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_ACCESS_KEY")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .getOrCreate()
)

bucket_name = "your_bucket_name" #change to youor bucket name!
# Bronze Layer
bronze_customers_path = f"s3a://{bucket_name}/lakehouse/bronze/customers.parquet"
bronze_order_item_path = f"s3a://{bucket_name}/lakehouse/bronze/order_item.parquet"
bronze_orders_path = f"s3a://{bucket_name}/lakehouse/bronze/orders.parquet"

# Silver Layer
silver_customers_path = f"s3a://{bucket_name}/lakehouse/silver/customers.parquet"
silver_order_item_path = f"s3a://{bucket_name}/lakehouse/silver/order_item.parquet"
silver_orders_path = f"s3a://{bucket_name}/lakehouse/silver/orders.parquet"

# Gold Layer
gold_final_df_path = f"s3a://{bucket_name}/lakehouse/gold/final_df"

# Diretório local onde os arquivos JSON estão armazenados
local_landing_path = "/home/beatriz/Documentos/airflow/lakehouse/landing"  #Change to your local path

def passo1_bronze():
    # Carregando e escrevendo os dados da camada Bronze
    table1_customers = spark.read.json(os.path.join(local_landing_path, "customers.json"))
    table1_customers.coalesce(1).write.mode("overwrite").parquet(bronze_customers_path)

    table2_orderItem = spark.read.json(os.path.join(local_landing_path, "order_item.json"))
    table2_orderItem.coalesce(1).write.mode("overwrite").parquet(bronze_order_item_path)

    table3_orders = spark.read.json(os.path.join(local_landing_path, "orders.json"))
    table3_orders.coalesce(1).write.mode("overwrite").parquet(bronze_orders_path)

def passo2_silver():
    # Transformação da camada Bronze para Silver
    table1_customers = spark.read.parquet(bronze_customers_path)
    newColumnNames_table1 = [col.replace('customer_', '') for col in table1_customers.columns]
    table1_customers = table1_customers.toDF(*newColumnNames_table1)
    table1_customers.coalesce(1).write.mode("overwrite").parquet(silver_customers_path)

    table2_orderItem = spark.read.parquet(bronze_order_item_path)
    newColumnNames_table2 = [col.replace('order_item_', '') for col in table2_orderItem.columns]
    table2_orderItem = table2_orderItem.toDF(*newColumnNames_table2)
    table2_orderItem.coalesce(1).write.mode("overwrite").parquet(silver_order_item_path)

    table3_orders = spark.read.parquet(bronze_orders_path)
    newColumnNames_table3 = [col.replace('order_', '') for col in table3_orders.columns]
    table3_orders = table3_orders.toDF(*newColumnNames_table3)
    table3_orders.coalesce(1).write.mode("overwrite").parquet(silver_orders_path)

def passo3_gold():
    # Transformação da camada Silver para Gold
    table1_customers = spark.read.parquet(silver_customers_path)
    table1_customers = table1_customers.withColumnRenamed("id", "id_customer")

    table2_orderItem = spark.read.parquet(silver_order_item_path)
    table2_orderItem = table2_orderItem.withColumnRenamed("id", "id_orderItem")

    table3_orders = spark.read.parquet(silver_orders_path)
    table3_orders = table3_orders.withColumnRenamed("id", "id_orders")

    customers_columns = table1_customers.select("city", "state", "id_customer")
    orderItem_columns = table2_orderItem.select("quantity", "subtotal", "order_id")

    customers_orders_join = table3_orders.join(
        customers_columns, table3_orders.customer_id == customers_columns.id_customer
    )
    final_df = customers_orders_join.join(
        orderItem_columns, customers_orders_join.id_orders == orderItem_columns.order_id
    )

    final_df = final_df.select("city", "state", "quantity", "subtotal")
    final_df.coalesce(1).write.mode("overwrite").parquet(gold_final_df_path)