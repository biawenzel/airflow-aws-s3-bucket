from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Medallion Architecture").getOrCreate()

def passo1_bronze():
    table1_customers = spark.read.json('/home/beatriz/Documentos/airflow/lakehouse/landing/customers.json')
    table1_customers.coalesce(1).write.parquet('/home/beatriz/Documentos/airflow/lakehouse/bronze/customers.parquet', "overwrite")

    table2_orderItem = spark.read.json('/home/beatriz/Documentos/airflow/lakehouse/landing/order_item.json')
    table2_orderItem.coalesce(1).write.parquet('/home/beatriz/Documentos/airflow/lakehouse/bronze/order_item.parquet', "overwrite")

    table3_orders = spark.read.json('/home/beatriz/Documentos/airflow/lakehouse/landing/orders.json')
    table3_orders.coalesce(1).write.parquet('/home/beatriz/Documentos/airflow/lakehouse/bronze/orders.parquet', "overwrite")
    


def passo2_silver():
    table1_customers = spark.read.parquet('/home/beatriz/Documentos/airflow/lakehouse/bronze/customers.parquet/*.parquet')
    newColumnNames_table1 = [col.replace('customer_', '') for col in table1_customers.columns]
    table1_customers = table1_customers.toDF(*newColumnNames_table1)
    table1_customers.coalesce(1).write.parquet('/home/beatriz/Documentos/airflow/lakehouse/silver/customers.parquet', "overwrite")

    table2_orderItem = spark.read.parquet('/home/beatriz/Documentos/airflow/lakehouse/bronze/order_item.parquet/*.parquet')
    newColumnNames_table2 = [col.replace('order_item_', '') for col in table2_orderItem.columns]
    table2_orderItem = table2_orderItem.toDF(*newColumnNames_table2)
    table2_orderItem.coalesce(1).write.parquet('/home/beatriz/Documentos/airflow/lakehouse/silver/order_item.parquet', "overwrite")

    table3_orders = spark.read.parquet('/home/beatriz/Documentos/airflow/lakehouse/bronze/orders.parquet/*.parquet')
    newColumnNames_table3 = [col.replace('order_', '') for col in table3_orders.columns]
    table3_orders = table3_orders.toDF(*newColumnNames_table3)
    table3_orders.coalesce(1).write.parquet('/home/beatriz/Documentos/airflow/lakehouse/silver/orders.parquet', "overwrite")
    


def passo3_gold():
    table1_customers = spark.read.parquet('/home/beatriz/Documentos/airflow/lakehouse/silver/customers.parquet/*.parquet')
    table1_customers = table1_customers.withColumnRenamed("id", "id_customer")

    table2_orderItem = spark.read.parquet('/home/beatriz/Documentos/airflow/lakehouse/silver/order_item.parquet/*.parquet')
    table2_orderItem = table2_orderItem.withColumnRenamed("id", "id_orderItem")

    table3_orders = spark.read.parquet('/home/beatriz/Documentos/airflow/lakehouse/silver/orders.parquet/*.parquet')
    table3_orders = table3_orders.withColumnRenamed("id", "id_orders")

    customers_columns = table1_customers.select("city", "state", "id_customer")
    orderItem_columns = table2_orderItem.select("quantity", "subtotal", "order_id")

    customers_orders_join = table3_orders.join(customers_columns, table3_orders.customer_id==customers_columns.id_customer)
    final_df = customers_orders_join.join(orderItem_columns, customers_orders_join.id_orders==orderItem_columns.order_id)

    final_df = final_df.select("city", "state", "quantity", "subtotal")
    final_df.coalesce(1).write.parquet("/home/beatriz/Documentos/airflow/lakehouse/gold/final_df", "overwrite")
    


if __name__ == "__main__":
    passo1_bronze()
    passo2_silver()
    passo3_gold()

#spark.stop()