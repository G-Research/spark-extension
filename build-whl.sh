#!/bin/bash

set -eo pipefail

base=$(cd "$(dirname "$0")"; pwd)

version=$(grep --max-count=1 "<version>.*</version>" "$base/pom.xml" | sed -E -e "s/\s*<[^>]+>//g")
artifact_id=$(grep --max-count=1 "<artifactId>.*</artifactId>" "$base/pom.xml" | sed -E -e "s/\s*<[^>]+>//g")

rm -rf "$base/python/pyspark/jars/"
mkdir -p "$base/python/pyspark/jars/"
cp -v "$base/target/$artifact_id-$version.jar" "$base/python/pyspark/jars/"
if [ $(ls -1 "$base/python/pyspark/jars/" | wc -l) -ne 1 ]
then
  echo "There are more than one jar in '$base/python/pyspark/jars/'"
  ls -lah "$base/python/pyspark/jars/"
  exit 1
fi

pip install build
python -m build "$base/python/"

