#!/bin/bash

set -eo pipefail

base=$(cd "$(dirname "$0")"; pwd)

version=$(grep --max-count=1 "<version>.*</version>" "$base/pom.xml" | sed -E -e "s/\s*<[^>]+>//g")
artifact_id=$(grep --max-count=1 "<artifactId>.*</artifactId>" "$base/pom.xml" | sed -E -e "s/\s*<[^>]+>//g")

rm -rf "$base/python/pyspark/jars/$artifact_id-*.jar"

pip install build
python -m build "$base/python/"

# check for missing modules in whl file
pyversion=${version/SNAPSHOT/dev0}
pyversion=${pyversion//-/.}

missing="$(diff <(cd "$base/python"; find gresearch -type f | grep -v ".pyc$" | sort) <(unzip -l "$base/python/dist/pyspark_extension-${pyversion}-*.whl" | tail -n +4 | head -n -2 | sed -E -e "s/^ +//" -e "s/ +/ /g" | cut -d " " -f 4- | sort) | grep "^<" || true)"
if [ -n "$missing" ]
then
  echo "These files are missing from the whl file:"
  echo "$missing"
  exit 1
fi

jars=$(unzip -l "$base/python/dist/pyspark_extension-${pyversion}-*.whl" | grep ".jar" | wc -l)
if [ $jars -ne 1 ]
then
  echo "Expected exactly one jar in whl file, but $jars found!"
  exit 1
fi
