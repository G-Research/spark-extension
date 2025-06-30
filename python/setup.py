#!/usr/bin/env python3

#  Copyright 2023 G-Research
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import shutil
import subprocess
import sys
from pathlib import Path
from setuptools import setup
from setuptools.command.sdist import sdist


jar_version = '2.14.0-3.5-SNAPSHOT'
scala_version = '2.13.8'
scala_compat_version = '.'.join(scala_version.split('.')[:2])
spark_compat_version = jar_version.split('-')[1]
jar_file = f"spark-extension_{scala_compat_version}-{jar_version}.jar"
version = jar_version.replace('SNAPSHOT', 'dev0').replace('-', '.')

# read the contents of the README.md file
long_description = (Path(__file__).parent / "README.md").read_text()


class custom_sdist(sdist):
    def make_distribution(self):
        # build jar file via mvn if it does not exist
        # then copy the jar file from target/ into python/pyspark/jars/
        project_root = Path(__file__).parent.parent
        jar_src_path = project_root / "target" / jar_file
        jar_dst_path = project_root / "python" / "pyspark" / "jars" / jar_file

        if not jar_dst_path.exists():
            if not jar_src_path.exists():
                # first set version for scala sources
                set_version_command = ["set-version.sh", f"{spark_compat_version}.0", scala_version]
                # then package Scala sources
                mvn_command = ["mvn", "--batch-mode", "package", "-Dspotless.check.skip", "-DskipTests", "-Dmaven.test.skip=true"]

                print(f"setting versions spark={spark_compat_version} scala={scala_version}")
                print(' '.join(set_version_command))
                try:
                    subprocess.check_call(set_version_command, cwd=str(project_root.absolute()))
                except OSError as e:
                    raise RuntimeError(f'setting versions failed: {e}')

                print(f"building {jar_src_path}")
                print(' '.join(mvn_command))
                try:
                    subprocess.check_call(mvn_command, cwd=str(project_root.absolute()))
                except OSError as e:
                    raise RuntimeError(f'mvn command failed: {e}')

                if not jar_src_path.exists():
                    print(f"Building jar file succeeded but file does still not exist: {jar_src_path}")
                    sys.exit(1)

            print(f"copying {jar_src_path} -> {jar_dst_path}")
            jar_dst_path.parent.mkdir(exist_ok=True)
            shutil.copy2(jar_src_path, jar_dst_path)
            self._add_data_files([("pyspark.jars", "pyspark/jars", ".", [jar_file])])

        sdist.make_distribution(self)


setup(
    name="pyspark-extension",
    version=version,
    description="A library that provides useful extensions to Apache Spark.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Enrico Minack",
    author_email="github@enrico.minack.dev",
    url="https://github.com/G-Research/spark-extension",
    cmdclass={'sdist': custom_sdist},
    install_requires=["typing_extensions"],
    extras_require={
        "test": [
            "pandas>=1.0.5",
            "py4j",
            "pyarrow>=4.0.0",
            f"pyspark~={spark_compat_version}.0",
            "pytest",
            "unittest-xml-reporting",
        ],
    },
    packages=[
        "gresearch",
        "gresearch.spark",
        "gresearch.spark.diff",
        "gresearch.spark.diff.comparator",
        "gresearch.spark.parquet",
        "pyspark.jars",
    ],
    include_package_data=False,
    package_data={
        "pyspark.jars": [jar_file],
    },
    license="http://www.apache.org/licenses/LICENSE-2.0.html",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Typing :: Typed",
    ],
)
