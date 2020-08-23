#!/bin/bash

if [ $# -ne 2 ]
then
	echo "Provide the Spark and Scala version"
	exit 1
fi

spark=$1
scala=$2

spark_compat=${spark%.*}
scala_compat=${scala%.*}

scala_patch=${scala/*./}
spark_patch=${spark/*./}

echo "setting spark=$spark and scala=$scala"
sed -i -E \
 -e "s%^(  <artifactId>)([^_]+)[_0-9.]+(</artifactId>)$%\1\2_${scala_compat}\3%" \
 -e "s%^(  <version>)([^-]+)-[^-]+(.*</version>)$%\1\2-$spark_compat\3%" \
 -e "s%^(    <scala.compat.version>).+(</scala.compat.version>)$%\1${scala_compat}\2%" \
 -e "s%^(    <scala.version>\\\$\{scala.compat.version\}.).+(</scala.version>)$%\1$scala_patch\2%" \
 -e "s%^(    <spark.compat.version>).+(</spark.compat.version>)$%\1${spark_compat}\2%" \
 -e "s%^(    <spark.version>\\\$\{spark.compat.version\}.).+(</spark.version>)$%\1$spark_patch\2%" \
 pom.xml

