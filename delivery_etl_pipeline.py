import pendulum
import os
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, first, sum, when, split, lit, countDistinct

### Общие константы и настройки:
TARGET_DDS_SCHEMA = "delivery_dds"
TARGET_DMA_SCHEMA = "delivery_dma"
DATA_PATH = "/opt/airflow/data"
POSTGRES_CONN_STRING = "postgresql://user:password@postgres:5432/delivery_dwh"
JDBC_DRIVER_PATH = "/opt/airflow/jars/postgresql-42.7.1.jar"
POSTGRES_JDBC_URL = "jdbc:postgresql://postgres:5432/delivery_dwh"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# ================================================================================
# Загрузка нормализованных данных из .parquet файлов в DDS (слой детальных данных)
# ================================================================================
def DDS_Load_Task():
    """
    Очищает таблицы DDS, затем обрабатывает каждый Parquet-файл по отдельности,
    загружая данные во временные таблицы. В конце агрегирует данные из временных
    таблиц в финальные DDS-таблицы.
    """
    print("--- Starting DDS loading task with micro-batching ---")
    
    # Подготовка данных: очистка старых данных и создание временных таблиц 
    engine = create_engine(POSTGRES_CONN_STRING)
    with engine.connect() as connection:
        with connection.begin():
            print("--- Truncating final DDS tables ---")
            tables_to_truncate = [
                f"{TARGET_DDS_SCHEMA}.order_items"
                ,f"{TARGET_DDS_SCHEMA}.orders"
                ,f"{TARGET_DDS_SCHEMA}.users"
                ,f"{TARGET_DDS_SCHEMA}.stores"
                ,f"{TARGET_DDS_SCHEMA}.drivers"
                ,f"{TARGET_DDS_SCHEMA}.items"
            ]
            connection.execute(text(f"TRUNCATE TABLE {', '.join(tables_to_truncate)} RESTART IDENTITY CASCADE;"))
            
            print("--- Creating temporary tables ---")
            # Создаем временные таблицы, которые будут точной копией основных, но без связей
            connection.execute(text(f"DROP TABLE IF EXISTS {TARGET_DDS_SCHEMA}.temp_all_data;"))
            connection.execute(text(
                f"""
                CREATE TABLE {TARGET_DDS_SCHEMA}.temp_all_data (
                    order_id BIGINT, user_id BIGINT, user_phone VARCHAR(50), address_text TEXT,
                    created_at TIMESTAMP, paid_at TIMESTAMP, delivery_started_at TIMESTAMP,
                    delivered_at TIMESTAMP, canceled_at TIMESTAMP, payment_type VARCHAR(50),
                    item_id BIGINT, item_title VARCHAR(255), item_category VARCHAR(255),
                    item_quantity INT, item_price DECIMAL(10, 2), item_canceled_quantity INT,
                    item_replaced_id BIGINT, order_discount DECIMAL(10, 2), item_discount DECIMAL(10, 2),
                    order_cancellation_reason VARCHAR(255), driver_id BIGINT, driver_phone VARCHAR(50),
                    delivery_cost DECIMAL(10, 2), store_id BIGINT, store_address TEXT
                );
                """
            ))
    engine.dispose()

    # --- Обработка каждого файла в цикле ---
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("ParquetToDDSLoad")
            .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH)
            .config("spark.driver.memory", "4g") 
            .master("local[*]")
            .getOrCreate()
        )
        
        parquet_files = [f for f in os.listdir(DATA_PATH) if f.endswith('.parquet')]
        print(f"--- Found {len(parquet_files)} parquet files to process ---")

        for i, file_name in enumerate(parquet_files):
            file_path = os.path.join(DATA_PATH, file_name)
            print(f"--- Processing file {i+1}/{len(parquet_files)}: {file_name} ---")
            
            df = spark.read.parquet(file_path)
            
            dedup_window = Window.partitionBy("order_id", "item_id").orderBy(col("item_quantity").desc())
            deduplicated_df = df.withColumn("duplicates_rn", row_number().over(dedup_window)) \
                                .filter(col("duplicates_rn") == 1).drop("duplicates_rn")
            
            deduplicated_df.write.jdbc(
                url=POSTGRES_JDBC_URL,
                table=f"{TARGET_DDS_SCHEMA}.temp_all_data",
                mode="append",
                properties=POSTGRES_PROPERTIES
            )
            print(f"--- Finished processing {file_name} ---")
            
    finally:
        if spark:
            spark.stop()

    # Финальная загрузка из временной таблицы в DDS средствами SQL 
    print("--- Starting final aggregation from temporary table to DDS ---")
    engine = create_engine(POSTGRES_CONN_STRING)
    with engine.connect() as connection:
        with connection.begin():
            print("--- Loading dimension tables ---")
            connection.execute(text(f"""
                INSERT INTO {TARGET_DDS_SCHEMA}.users (user_id, user_phone)
                SELECT DISTINCT ON (user_id) user_id, user_phone FROM {TARGET_DDS_SCHEMA}.temp_all_data WHERE user_id IS NOT NULL;
                
                INSERT INTO {TARGET_DDS_SCHEMA}.stores (store_id, store_address)
                SELECT DISTINCT ON (store_id) store_id, store_address FROM {TARGET_DDS_SCHEMA}.temp_all_data WHERE store_id IS NOT NULL;

                INSERT INTO {TARGET_DDS_SCHEMA}.drivers (driver_id, driver_phone)
                SELECT DISTINCT ON (driver_id) driver_id, driver_phone FROM {TARGET_DDS_SCHEMA}.temp_all_data WHERE driver_id IS NOT NULL;

                INSERT INTO {TARGET_DDS_SCHEMA}.items (item_id, item_title, item_category)
                SELECT DISTINCT ON (item_id) item_id, item_title, item_category FROM {TARGET_DDS_SCHEMA}.temp_all_data WHERE item_id IS NOT NULL;
            """))
            
            print("--- Loading fact tables ---")
            connection.execute(text(f"""
                WITH ranked_orders AS (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY created_at DESC, item_quantity DESC) as rn
                    FROM {TARGET_DDS_SCHEMA}.temp_all_data
                )
                INSERT INTO {TARGET_DDS_SCHEMA}.orders (order_id, user_id, store_id, driver_id, address_text, created_at, paid_at, delivery_started_at, delivered_at, canceled_at, payment_type, order_discount, delivery_cost, order_cancellation_reason)
                SELECT order_id, user_id, store_id, driver_id, address_text, created_at, paid_at, delivery_started_at, delivered_at, canceled_at, payment_type, order_discount, delivery_cost, order_cancellation_reason
                FROM ranked_orders WHERE rn = 1;

                INSERT INTO {TARGET_DDS_SCHEMA}.order_items (order_id, item_id, item_quantity, item_price, item_discount, item_canceled_quantity, item_replaced_id)
                SELECT order_id, item_id, item_quantity, item_price, item_discount, item_canceled_quantity, item_replaced_id FROM {TARGET_DDS_SCHEMA}.temp_all_data;
            """))
            
            print("--- Cleaning up temporary table ---")
            connection.execute(text(f"DROP TABLE {TARGET_DDS_SCHEMA}.temp_all_data;"))
    
    engine.dispose()
    print("--- DDS loading task finished successfully! ---")

# =============================================================================
# Расчет витрин данных на основе DDS слоя 
# =============================================================================
def DMA_Load_Task():
    """
    Выполняется расчет витрин 
    Результат записывается в слой DMA.
    """
    print("--- Starting data marts calculation task ---")
    engine = create_engine(POSTGRES_CONN_STRING)

    # Расчет и вставка в витрину заказов
    orders_report_sql = f"""
    INSERT INTO {TARGET_DMA_SCHEMA}.orders_report (
        report_date, store_id, store_city, revenue_sum, turnover_sum, profit_sum,
        created_orders_count, delivered_orders_count, canceled_orders_count,
        cancellations_after_delivery_count, service_error_cancellations_count,
        unique_buyers_count, avg_check, orders_per_buyer, revenue_per_buyer,
        driver_changes_count, active_drivers_count
    )
    WITH order_agg AS (
        -- Предварительная агрегация по товарам в заказе
        SELECT
            order_id,
            SUM(item_price * item_quantity) AS turnover_gross,
            SUM(item_price * (item_quantity - item_canceled_quantity)) AS revenue_gross
        FROM {TARGET_DDS_SCHEMA}.order_items
        GROUP BY order_id
    ),
    final_agg AS (
        SELECT
            o.created_at::date AS report_date,
            s.store_id,
            split_part(s.store_address, ',', 1) AS store_city,
            -- Расчет метрик
            SUM(CASE WHEN o.paid_at IS NOT NULL THEN oa.revenue_gross - o.order_discount ELSE 0 END) AS revenue_sum,
            SUM(oa.turnover_gross - o.order_discount) AS turnover_sum,
            SUM(CASE WHEN o.paid_at IS NOT NULL THEN o.delivery_cost ELSE 0 END) AS total_delivery_cost,
            COUNT(DISTINCT o.order_id) AS created_orders_count,
            COUNT(DISTINCT CASE WHEN o.delivered_at IS NOT NULL THEN o.order_id END) AS delivered_orders_count,
            COUNT(DISTINCT CASE WHEN o.canceled_at IS NOT NULL THEN o.order_id END) AS canceled_orders_count,
            COUNT(DISTINCT o.user_id) AS unique_buyers_count,
            -- Добавленные метрики
            COUNT(DISTINCT CASE WHEN o.canceled_at IS NOT NULL AND o.delivered_at IS NOT NULL THEN o.order_id END) AS cancellations_after_delivery_count,
            COUNT(DISTINCT CASE WHEN o.order_cancellation_reason IN ('Ошибка приложения', 'Проблемы с оплатой') THEN o.order_id END) AS service_error_cancellations_count,
            COUNT(DISTINCT o.driver_id) AS active_drivers_count
        FROM {TARGET_DDS_SCHEMA}.orders o
        JOIN {TARGET_DDS_SCHEMA}.stores s ON o.store_id = s.store_id
        JOIN order_agg oa ON o.order_id = oa.order_id
        GROUP BY report_date, s.store_id, store_city
    )
    SELECT
        report_date, store_id, store_city, revenue_sum, turnover_sum,
        (revenue_sum - total_delivery_cost) AS profit_sum,
        created_orders_count, delivered_orders_count, canceled_orders_count,
        cancellations_after_delivery_count, service_error_cancellations_count,
        unique_buyers_count,
        COALESCE(revenue_sum / NULLIF(delivered_orders_count, 0), 0) AS avg_check,
        COALESCE(created_orders_count::decimal / NULLIF(unique_buyers_count, 0), 0) AS orders_per_buyer,
        COALESCE(revenue_sum / NULLIF(unique_buyers_count, 0), 0) AS revenue_per_buyer,
        0 AS driver_changes_count, -- Заглушка по условию
        active_drivers_count
    FROM final_agg;
    """

    # Расчет и вставка в витрину товаров
    items_report_sql = f"""
    INSERT INTO {TARGET_DMA_SCHEMA}.items_report (
        report_date, store_id, store_city, item_id, item_category, item_turnover_sum,
        ordered_items_quantity, canceled_items_quantity, orders_with_item_count,
        orders_with_item_cancellation_count
    )
    SELECT
        o.created_at::date AS report_date,
        o.store_id,
        split_part(s.store_address, ',', 1) AS store_city,
        oi.item_id,
        i.item_category,
        SUM(oi.item_price * oi.item_quantity) AS item_turnover_sum,
        SUM(oi.item_quantity) AS ordered_items_quantity,
        SUM(oi.item_canceled_quantity) AS canceled_items_quantity,
        COUNT(DISTINCT o.order_id) AS orders_with_item_count,
        COUNT(DISTINCT CASE WHEN oi.item_canceled_quantity > 0 THEN o.order_id END) AS orders_with_item_cancellation_count
    FROM {TARGET_DDS_SCHEMA}.orders o
    JOIN {TARGET_DDS_SCHEMA}.order_items oi ON o.order_id = oi.order_id
    JOIN {TARGET_DDS_SCHEMA}.stores s ON o.store_id = s.store_id
    JOIN {TARGET_DDS_SCHEMA}.items i ON oi.item_id = i.item_id
    GROUP BY o.created_at::date,
        o.store_id,
        split_part(s.store_address, ',', 1),
        oi.item_id,
        i.item_category;
    """
    
    with engine.connect() as connection:
        with connection.begin():
            print("--- Truncating report tables ---")
            connection.execute(text(f"TRUNCATE TABLE {TARGET_DMA_SCHEMA}.orders_report, {TARGET_DMA_SCHEMA}.items_report RESTART IDENTITY;"))
            
            print("--- Calculating and inserting into orders_report ---")
            connection.execute(text(orders_report_sql))
            
            print("--- Calculating and inserting into items_report ---")
            connection.execute(text(items_report_sql))
    
    print("--- Data marts calculation task finished successfully! ---")

# =============================================================================
# Определение DAG'а
# =============================================================================
with DAG(
    dag_id='delivery_etl_pipeline',
    description='Полный ETL-процесс: загрузка данных в DDS -> DMA',
    start_date=pendulum.datetime(2024, 12, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['project', 'pyspark', 'layered-architecture'],
) as dag:
    # Задача 1: Загрузка нормализованных данных в DDS
    DDS_Load = PythonOperator(
        task_id='DDS_Load_Task_id',
        python_callable=DDS_Load_Task,
    )
    # Задача 2: Расчет витрин на основе данных в DDS
    DMA_Load = PythonOperator(
        task_id='DMA_Load_Task_id',
        python_callable=DMA_Load_Task,
    )
    # Последовательность выполнения DAG'ов:
    DDS_Load >> DMA_Load
    