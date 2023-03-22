#!/bin/bash

if [ $# -eq 1 ]
then
    IFS=-
    read version flavour <<< "$1"

    echo "setting version=$version${flavour:+ with }$flavour"

    sed -i -E \
        -e "s%^(  <version>)[^-]+-([^-]+).*(</version>)$%\1$version-\2${flavour:+-}$flavour\3%" \
        pom.xml

    version=$(grep -m 1 version pom.xml | sed "s/\s*<[^>]*>\s*//g")

    sed -i -E \
        -e "s/(jar_version *= *).*/\1'$version'/" \
        python/setup.py
elif [ $# -eq 2 ]
then
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

    version=$(grep -m 1 version pom.xml | sed "s/\s*<[^>]*>\s*//g")

    sed -i -E \
        -e "s/(jar_version *= *).*/\1'$version'/" \
        -e "s/(scala_version *= *).*/\1'$scala'/" \
        python/setup.py
else
    echo "Provide the Spark-Extension version (e.g. 2.5.0 or 2.5.0-SNAPSHOT), or the Spark and Scala version"
    exit 1
fi

