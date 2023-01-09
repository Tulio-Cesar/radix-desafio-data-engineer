from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, LongType, DateType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("airflow_app") \
    .config('spark.executor.memory', '6g') \
    .config('spark.driver.memory', '6g') \
    .config("spark.driver.maxResultSize", "1048MB") \
    .config("spark.port.maxRetries", "100") \
    .getOrCreate()


####processamento e inserção da camada trusted
def insert_trust():
    def read_csv(path):
        csv_df = spark.read.csv(f'/home/airflow/datalake/raw/covid19/time_series_covid19_{path}_global.csv', sep=',', inferSchema=True, header=True)

        default = ['Province/State', 'Country/Region', 'Lat', 'Long']
        dynamic = [col_name for col_name in csv_df.schema.names if col_name not in default]
        stack = ''
        for value in dynamic:
            stack += f",'{value}', `{value}`"

        pivot_df = csv_df.select('Province/State', 'Country/Region', 'Lat', 'Long', expr(f'stack ({len(dynamic)} {stack}) as (Date, quantity_of_{path})'))    
        return pivot_df

    csv_confirmed_df = read_csv('confirmed')
    csv_deaths_df = read_csv('deaths')
    csv_recovered_df = read_csv('recovered')  


    join_df = csv_confirmed_df.join(csv_deaths_df, ['Country/Region', 'Lat', 'Long', 'Date'], how = 'inner').drop(csv_deaths_df['Province/State'])
    join_df = join_df.join(csv_recovered_df, ['Country/Region', 'Lat', 'Long', 'Date'], how = 'inner').drop(csv_recovered_df['Province/State'])
    
    complete_df = join_df.select(col("Country/Region").alias("pais"),
                    col("Province/State").alias("estado"),
                    col("Lat").alias("latitude"),
                    col("Long").alias("longitude"),
                    to_timestamp("Date",  "M/d/yy").alias("data"),
                    col("quantity_of_confirmed").alias("quantidade_confirmados").cast(LongType()),
                    col("quantity_of_deaths").alias("quantidade_mortes").cast(LongType()),
                    col("quantity_of_recovered").alias("quantidade_recuperados").cast(LongType()))
    complete_df = complete_df.withColumn("year", date_format(col("data"), "yyyy").cast(IntegerType()))   \
                        .withColumn("month", date_format(col("data"), "MM").cast(IntegerType()))

    complete_df.coalesce(1) \
                .write.option("header",True) \
                .partitionBy("year", "month") \
                .mode("overwrite") \
                .parquet("/home/airflow/datalake/trusted")


#processamento e inserção da camada refined
def insert_refined():
    pqt_read_df = spark.read.parquet("/home/airflow/datalake/trusted")
    
    days = lambda i: i * 86400
    windowSpec = (Window().partitionBy([col("pais"), col("estado")]).orderBy(pqt_read_df.data.cast(TimestampType()).cast(LongType())).rangeBetween(-days(6), 0))
    rol_avg_df = pqt_read_df.withColumn("media_movel_confirmados", round(avg("quantidade_confirmados").over(windowSpec), 2).cast(LongType()))    \
                            .withColumn("media_movel_mortes", round(avg("quantidade_mortes").over(windowSpec), 2).cast(LongType()))   \
                            .withColumn("media_movel_recuperados", round(avg("quantidade_recuperados").over(windowSpec), 2).cast(LongType()))   \
                            .select(col("pais"), col("data"), col("media_movel_confirmados"), col("media_movel_mortes"), col("media_movel_recuperados"), col("year")) 

    rol_avg_df.coalesce(1)\
                .write.option("header",True) \
                .partitionBy("year") \
                .mode("overwrite") \
                .parquet("/home/airflow/datalake/refined")



#### configuração e criação das tasks

default_args = {
    "owner": "tuliocesarleite",
    "email": ["tuliocesar.ls@hotmail.com"],
    'start_date': days_ago(1),
    'email_on_failure' : False

}

with DAG(
    dag_id="dag_load_covid19",
    schedule_interval="* 7 * * *",
    default_args=default_args,
    catchup=False,
    tags=["teste", "covid_19"],
) as dag:
    task_trust = PythonOperator(
        task_id="task_insert_trust",
        python_callable=insert_trust
    )
    
    task_refined = PythonOperator(
        task_id="task_insert_refined",
        python_callable=insert_refined
    )
            
task_trust >> task_refined                