from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

def convertCSVToParquet():
    conf = SparkConf().setMaster("local[1]").setAppName("CSV2Parquet")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField("dept_no", StringType(), False),
        StructField("dept_name", StringType(), False)])

    rdd = sc.textFile("s3://testhasbucket/Spark/csv/departments.csv").map(lambda line: line.split(","))

    df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet('s3://testhasbucket/Spark/parquet/ParquetData')
# Code Reference : http://blogs.quovantis.com/how-to-convert-csv-to-parquet-files/
if __name__ == "__main__":
    convertCSVToParquet()