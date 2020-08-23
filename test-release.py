from pyspark import SparkConf
from pyspark.sql import SparkSession

import gresearch.spark.diff

conf = SparkConf().setAppName('integration test').setMaster('local[2]')
conf = conf.setAll([
    ('spark.ui.showConsoleProgress', 'false'),
    ('spark.locality.wait', '0'),
])

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()

left = spark.createDataFrame([(1, "one"), (2, "two"), (3, "three")], ["id", "value"])
right = spark.createDataFrame([(1, "one"), (2, "Two"), (4, "four")], ["id", "value"])

left.diff(right).show()

