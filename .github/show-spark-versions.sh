#!/bin/bash

base=$(cd "$(dirname "$0")"; pwd)

grep -- "-version" "$base"/workflows/prime-caches.yml | sed -e "s/ -//g" -e "s/ //g" -e "s/'//g" | grep -v -e "matrix" -e "]" | while read line
do
  IFS=":" read var compat_version <<< "$line"
  if [[ "$var" == "spark-compat-version" ]]
  then
    while read line
    do
      IFS=":" read var patch_version <<< "$line"
      if [[ "$var" == "spark-patch-version" ]]
      then
        echo -n "spark-version: $compat_version.$patch_version"
        read line
        if [[ "$line" == "spark-snapshot-version:true" ]]
        then
          echo "-SNAPSHOT"
        else
          echo
        fi
        break
      fi
    done
  fi
done > "$base"/workflows/prime-caches.yml.tmp

grep spark-version "$base"/workflows/*.yml "$base"/workflows/prime-caches.yml.tmp | cut -d : -f 2- | sed -e "s/^[ -]*//" -e "s/'//g" -e 's/{"params": {"//g' -e 's/"//g' -e "s/,.*//" | grep "^spark-version" | grep -v "matrix" | sort | uniq

