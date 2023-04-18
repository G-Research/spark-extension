package uk.co.gresearch.spark

import java.util.Properties

trait BuildVersion {
  val propertyFileName = "spark-extension-build.properties"

  lazy val props: Properties = {
    val properties = new Properties
    val in = Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertyFileName))
    if (in.isEmpty) {
      throw new RuntimeException(s"Property file $propertyFileName not found in class path")
    }

    properties.load(in.get)
    properties
  }

  lazy val BuildSparkMajorVersion: Int = props.getProperty("spark.major.version").toInt
  lazy val BuildSparkMinorVersion: Int = props.getProperty("spark.minor.version").toInt
  lazy val BuildSparkPatchVersion: Int = props.getProperty("spark.patch.version").toInt
  lazy val BuildSparkCompatVersionString: String = props.getProperty("spark.compat.version")

  val BuildSparkVersion: (Int, Int, Int) = (BuildSparkMajorVersion, BuildSparkMinorVersion, BuildSparkPatchVersion)
  val BuildSparkCompatVersion: (Int, Int) = (BuildSparkMajorVersion, BuildSparkMinorVersion)
}
