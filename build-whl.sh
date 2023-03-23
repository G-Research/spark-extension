mkdir -p python/pyspark/jars/
cp -v target/spark-extension_*-*.jar python/pyspark/jars/
pip install build
python -m build python/

