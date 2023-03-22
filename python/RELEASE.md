# Release pyspark-extension package

- Run mvn package in parent directory
- Copy target/spark-extension_{scala.compat.version}-{version}.jar to python/pyspark/jars/
- Go into python
- Build Python package

mvn package
mkdir -p python/pyspark/jars/
cp -v target/spark-extension_*-*.jar python/pyspark/jars/
cd python
python -m build

