#!/bin/bash

set -eo pipefail

version=$(grep --max-count=1 "<version>.*</version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")
spark_compat=$(grep --max-count=1 spark.compat.version pom.xml | sed -E -e "s/\s*<[^>]+>//g")
spark=$(grep --max-count=1 spark.version pom.xml | sed -E -e "s/\s*<[^>]+>//g" -e "s/\\\$\{spark.compat.version\}/$spark_compat/")
scala_compat=$(grep --max-count=1 scala.compat.version pom.xml | sed -E -e "s/\s*<[^>]+>//g")

echo
echo "Testing Spark $spark and Scala $scala_compat"
echo

if [ ! -e "spark-$spark-$scala_compat" ]
then
	if [ "$scala_compat" == "2.12" ]
	then
		if [ "$spark_compat" == "3.3" ]
		then
			hadoop="hadoop2"
		else
			hadoop="hadoop2.7"
		fi
	elif [ "$scala_compat" == "2.13" ]
	then
		if [ "$spark_compat" == "3.3" ]
		then
			hadoop="hadoop3.3-scala2.13"
		else
			hadoop="hadoop3.2-scala2.13"
		fi
	else
		hadoop="without-hadoop"
	fi
	wget --progress=dot:giga https://archive.apache.org/dist/spark/spark-$spark/spark-$spark-bin-$hadoop.tgz -O - | tar -xzC .
	ln -s spark-$spark-bin-$hadoop spark-$spark-$scala_compat
fi

spark-$spark-$scala_compat/bin/spark-shell --packages uk.co.gresearch.spark:spark-extension_$scala_compat:$version --repositories https://oss.sonatype.org/content/groups/staging/ < test-release.scala

if [ ! -e "venv" ]
then
	virtualenv -p python3 venv
	source venv/bin/activate
	pip install python/requirements-${spark_compat}_${scala_compat}.txt
	deactivate
fi

source venv/bin/activate
spark-$spark-$scala_compat/bin/spark-submit --packages uk.co.gresearch.spark:spark-extension_$scala_compat:$version test-release.py
deactivate

echo -e "\u001b[32;1mSUCCESS\u001b[0m"
