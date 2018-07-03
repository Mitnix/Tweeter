""" Import the for pyspark, dataframes """
from pyspark.sql import SparkSession, HiveContext
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys

__author__ = "Mithun.Balegadde"

""" Read table from MSSQL and ingest into hive table"""


""" Spark Configurations for setup application name,
 Set master,ui port and spark scheduler """
conf = SparkConf().setAppName("WintropIngest") \
                  .setMaster("local[*]") \
                  .set("spark.ui.port", "18080") \
                  .set("spark.scheduler.mode", "FAIR")

""" Set spark context and hive communication uris """
sc = SparkContext("local[*]", "WintropIngest") \
    .setSystemProperty("hive.metastore.uris", "thrift://mn-bddevm03.corp.***.biz:9083")

""" Set Spark Session, Application name, Enable Hive dynamic partition and mode as nonstric """
spark = SparkSession \
    .builder \
    .appName("WinthropIngest") \
    .config("spark.sql.option", "2") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("hive.exec.dynamic.partition","true") \
    .enableHiveSupport() \
    .getOrCreate()
#sc.setLogLevel("Error")

SourceTable = sys.argv[1]
TargetTable = sys.argv[2]

""" Configure to pass driver config, Database, Username, Password, Table name """
Mssql = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://mtkstagesql6.lwh.net;database=LW_Leo;user=****;password==****") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("dbtable", SourceTable) \
    .load()

""" Define structure for dataframe """
schema = StructType([StructField('checkrequestinvoiceid', StringType()),
                     StructField('checkrequestid',	StringType()),
                     StructField('vendorinvoiceid',	StringType()),
                     StructField('isactive',	StringType()),
                     StructField('docexamineroutinerun',	StringType()),
                     StructField('lastmodifieduserid',	StringType()),
                     StructField('lastmodifieddate',	StringType()),
                     StructField('issaved',	StringType()),
                     StructField('etl_run_dt_yyyymm',	StringType()),
                     StructField('etl_run_dt_dd',	StringType())])

""" Create temp table """
Mssql.registerTempTable("Check")

Qry = spark.sql("select * from Check limit 5")

Qry.printSchema()

""" Add column to dataframe in this case the columns are partition columns """
DF1 = Qry.withColumn("etl_run_dt_yyyymm",  (F.date_format((F.current_date()), "yyyy/MM")).cast("string"))
DF2 = DF1.withColumn("etl_run_dt_dd", (F.dayofmonth(F.current_date())).cast("string"))

""" Insert into Hive Table, Schema EDP_LANDING """
# DF2.write.mode("overwrite").insertInto("edp_landing.checkrequest_invoice")
DF2.write.mode("overwrite").insertInto(TargetTable)

""" Create and ingest csv output into hadoop """
Insrt = DF2.write.csv("hdfs://dev01hdpsvc:8020/data/landing/wintst/winthrop.csv")

sc.stop()
