from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("spark_app").getOrCreate()

    def func(df):
        return df

    from gresearch.spark import install_pip_package

    spark.install_pip_package("pandas", "pyarrow")
    spark.range(0, 3, 1, 5).mapInPandas(func, "id long").show()

if __name__ == "__main__":
    main()
