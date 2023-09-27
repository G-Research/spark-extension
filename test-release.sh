#!/bin/bash

set -eo pipefail

version=$(grep --max-count=1 "<version>.*</version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")

spark_major=$(grep --max-count=1 "<spark.major.version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")
spark_minor=$(grep --max-count=1 "<spark.minor.version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")
spark_patch=$(grep --max-count=1 "<spark.patch.version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")
spark_compat="$spark_major.$spark_minor"
spark="$spark_major.$spark_minor.$spark_patch"

scala_major=$(grep --max-count=1 "<scala.major.version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")
scala_minor=$(grep --max-count=1 "<scala.minor.version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g")
scala_compat="$scala_major.$scala_minor"

echo
echo "Testing Spark $spark and Scala $scala_compat"
echo

if [ ! -e "spark-$spark-$scala_compat" ]
then
    if [[ "$scala_compat" == "2.12" ]]
    then
        if [[ "$spark_compat" < "3.3" ]]
        then
            hadoop="hadoop2.7"
        else
            hadoop="hadoop3"
        fi
    elif [[ "$scala_compat" == "2.13" ]]
    then
        if [[ "$spark_compat" < "3.3" ]]
        then
            hadoop="hadoop3.2-scala2.13"
        else
            hadoop="hadoop3-scala2.13"
        fi
    else
        hadoop="without-hadoop"
    fi
    wget --progress=dot:giga https://archive.apache.org/dist/spark/spark-$spark/spark-$spark-bin-$hadoop.tgz -O - | tar -xzC .
    ln -s spark-$spark-bin-$hadoop spark-$spark-$scala_compat
fi

echo "Testing Scala"
spark-$spark-$scala_compat/bin/spark-shell --packages uk.co.gresearch.spark:spark-extension_$scala_compat:$version --repositories https://oss.sonatype.org/content/groups/staging/ < test-release.scala

echo "Testing Python with Scala package"
spark-$spark-$scala_compat/bin/spark-submit --packages uk.co.gresearch.spark:spark-extension_$scala_compat:$version test-release.py

if [ "$scala_compat" == "2.12" ]
then
    echo "Testing Python with whl package"
    if [ -e "venv" ]; then rm -rf venv; fi
    virtualenv -p python3.10 venv
    source venv/bin/activate
    pip install -r python/requirements-${spark_compat}_$scala_compat.txt
    pip install python/dist/pyspark_extension-${version/-*/}.$spark_compat${version/*-SNAPSHOT/.dev0}-py3-none-any.whl
    python3 test-release.py
fi


echo -e "\u001b[32;1mSUCCESS\u001b[0m"
