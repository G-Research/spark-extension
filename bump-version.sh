#!/bin/bash
#
# Copyright 2020 G-Research
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script to prepare release, see RELEASE.md for details

set -e -o pipefail

# check for clean git status
readarray -t git_status < <(git status -s --untracked-files=no 2>/dev/null)
if [ ${#git_status[@]} -gt 0 ]
then
  echo "There are pending git changes:"
  for (( i=0; i<${#git_status[@]}; i++ )); do echo "${git_status[$i]}" ; done
  exit 1
fi

function next_version {
  local version=$1
  local branch=$2

  patch=${version/*./}
  majmin=${version%.${patch}}

  if [[ $branch == "master" ]]
  then
    # minor version bump
    if [[ $version != *".0" ]]
    then
      echo "version is patch version, should be M.m.0: $version" >&2
      exit 1
    fi
    maj=${version/.*/}
    min=${majmin#${maj}.}
    next=${maj}.$((min+1)).0
    echo "$next"
  else
    # patch version bump
    next=${majmin}.$((patch+1))
    echo "$next"
  fi
}

# get release and next version
version=$(grep --max-count=1 "<version>.*</version>" pom.xml | sed -E -e "s/\s*<[^>]+>//g" -e "s/-SNAPSHOT//" -e "s/-[0-9.]+//g")
branch=$(git rev-parse --abbrev-ref HEAD)
next=$(next_version "$version" "$branch")

# bump the version
echo "Bump version to $next"
sed -i "1,10s/$version/$next-SNAPSHOT/" pom.xml examples/scala/pom.xml

echo "this sed is not working"
exit 1

# commit changes to local repo
echo
echo "Committing release to local git"
git commit -a -m "Post-release version bump to $next"
git show HEAD
echo

# push version bump to origin
echo "Pushing release commit and tag to origin"
git push origin "master"
echo
