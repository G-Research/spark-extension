#!/bin/bash

set -eo pipefail

version=$(grep --max-count=1 "<version>.*</version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")
spark_compat=$(grep --max-count=1 spark.compat.version pom.xml | sed -E -e "s/\s*<[^>]+>//g")
spark=$(grep --max-count=1 spark.version pom.xml | sed -E -e "s/\s*<[^>]+>//g" -e "s/\\\$\{spark.compat.version\}/$spark_compat/")
scala_compat=$(grep --max-count=1 scala.compat.version pom.xml | sed -E -e "s/\s*<[^>]+>//g")

echo
echo "Testing Spark $spark and Scala $scala"
echo

if [ ! -e "spark-$spark" ]
then
	wget --progress=dot:giga https://archive.apache.org/dist/spark/spark-$spark/spark-$spark-bin-hadoop2.7.tgz -O - | tar -xzC .
	ln -s spark-$spark-bin-hadoop2.7 spark-$spark
fi

spark-$spark/bin/spark-shell --packages uk.co.gresearch.spark:spark-extension_$scala_compat:$version < test-release.scala
spark-$spark/bin/spark-submit --packages uk.co.gresearch.spark:spark-extension_$scala_compat:$version test-release.py

echo -e "\u001b[32;1mSUCCESS\u001b[0m"
