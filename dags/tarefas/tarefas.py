import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Medallion Architecture").getOrCreate()

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Definindo os caminhos de saída para a camada Bronze
bronze_customers_path = os.path.join(base_path, 'lakehouse/bronze/customers.parquet')
bronze_order_item_path = os.path.join(base_path, 'lakehouse/bronze/order_item.parquet')
bronze_orders_path = os.path.join(base_path, 'lakehouse/bronze/orders.parquet')

# Definindo os caminhos de saída para a camada Silver
silver_customers_path = os.path.join(base_path, 'lakehouse/silver/customers.parquet')
silver_order_item_path = os.path.join(base_path, 'lakehouse/silver/order_item.parquet')
silver_orders_path = os.path.join(base_path, 'lakehouse/silver/orders.parquet')

def passo1_bronze():
    # Carregando e escrevendo os dados da camada Bronze
    table1_customers = spark.read.json(os.path.join(base_path, 'lakehouse/landing/customers.json'))
    table1_customers.coalesce(1).write.mode("overwrite").parquet(bronze_customers_path)
    
    table2_orderItem = spark.read.json(os.path.join(base_path, 'lakehouse/landing/order_item.json'))
    table2_orderItem.coalesce(1).write.mode("overwrite").parquet(bronze_order_item_path)

    table3_orders = spark.read.json(os.path.join(base_path, 'lakehouse/landing/orders.json'))
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
    # Definindo o caminho de saída para a camada Gold
    gold_final_df_path = os.path.join(base_path, 'lakehouse/gold/final_df')

    # Transformação da camada Silver para Gold
    table1_customers = spark.read.parquet(silver_customers_path)
    table1_customers = table1_customers.withColumnRenamed("id", "id_customer")

    table2_orderItem = spark.read.parquet(silver_order_item_path)
    table2_orderItem = table2_orderItem.withColumnRenamed("id", "id_orderItem")

    table3_orders = spark.read.parquet(silver_orders_path)
    table3_orders = table3_orders.withColumnRenamed("id", "id_orders")

    customers_columns = table1_customers.select("city", "state", "id_customer")
    orderItem_columns = table2_orderItem.select("quantity", "subtotal", "order_id")

    customers_orders_join = table3_orders.join(customers_columns, table3_orders.customer_id == customers_columns.id_customer)
    final_df = customers_orders_join.join(orderItem_columns, customers_orders_join.id_orders == orderItem_columns.order_id)

    final_df = final_df.select("city", "state", "quantity", "subtotal")
    final_df.coalesce(1).write.mode("overwrite").parquet(gold_final_df_path)

if __name__ == "__main__":
    passo1_bronze()  
    passo2_silver()  
    passo3_gold()    