# this requires parquet-hadoop-*-tests.jar
# fetch with mvn dependency:get -Dtransitive=false -Dartifact=org.apache.parquet:parquet-hadoop:1.16.0:jar:tests
from pathlib import Path
hadoop_parquet_tests = f"{Path.home()}/.m2/repository/org/apache/parquet/parquet-hadoop/1.16.0/parquet-hadoop-1.16.0-tests.jar"

from pyspark import SparkConf
from pyspark.sql import SparkSession

# noinspection PyUnresolvedReferences
import gresearch.spark.diff
import gresearch.spark.parquet


conf = SparkConf().setAppName('integration test').setMaster('local[2]')
conf = conf.setAll([
    ('spark.ui.showConsoleProgress', 'false'),
    ('spark.locality.wait', '0'),
    ('spark.jars', hadoop_parquet_tests),
    ('spark.driver.extraClassPath', hadoop_parquet_tests),
])

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

left = spark.createDataFrame([(1, "one"), (2, "two"), (3, "three")], ["id", "value"])
right = spark.createDataFrame([(1, "one"), (2, "Two"), (4, "four")], ["id", "value"])

left.diff(right).show()


for file in ["test.parquet", "nested.parquet", "encrypted1.parquet"]:
    print(file)
    path = f"src/test/files/{file}"
    spark.read.parquet_metadata(path).show()
    spark.read.parquet_schema(path).show()
    spark.read.parquet_blocks(path).show()
    spark.read.parquet_block_columns(path).show()
    if file != "encrypted1.parquet":
        spark.read.parquet_partitions(path).show()

# configure footer key only
hc = spark.sparkContext._jsc.hadoopConfiguration()
hc.set("parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
hc.set("parquet.encryption.kms.client.class", "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")
hc.set("parquet.encryption.key.list", "key:AAECAAECAAECAAECAAECAA==")

for file in ["encrypted1.parquet", "encrypted2.parquet"]:
    print(file)
    path = f"src/test/files/{file}"
    spark.read.parquet_metadata(path).show()
    spark.read.parquet_schema(path).show()
    spark.read.parquet_blocks(path).show()
    spark.read.parquet_block_columns(path).show()
    spark.read.parquet_partitions(path).show()