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

# check for missing modules in whl file
pyversion=${version/SNAPSHOT/dev0}
pyversion=${pyversion//-/.}

missing="$(diff <(cd $base/python; find gresearch -type f | grep -v ".pyc$" | sort) <(unzip -l $base/python/dist/pyspark_extension-${pyversion}-*.whl | tail -n +4 | head -n -2 | sed -E -e "s/^ +//" -e "s/ +/ /g" | cut -d " " -f 4- | sort) | grep "^<" || true)"
if [ -n "$missing" ]
then
  echo "These files are missing from the whl file:"
  echo "$missing"
  exit 1
fi
