package uk.co.gresearch.spark

import java.util.Properties

/**
 * Provides versions from build environment.
 */
trait BuildVersion {
  val propertyFileName = "spark-extension-build.properties"

  lazy val props: Properties = {
    val properties = new Properties

    val in = Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertyFileName))
    if (in.isEmpty) {
      throw new RuntimeException(s"Property file $propertyFileName not found in class path")
    }

    in.foreach(properties.load)
    properties
  }

  lazy val VersionString: String = props.getProperty("project.version")
  
  lazy val BuildSparkMajorVersion: Int = props.getProperty("spark.major.version").toInt
  lazy val BuildSparkMinorVersion: Int = props.getProperty("spark.minor.version").toInt
  lazy val BuildSparkPatchVersion: Int = props.getProperty("spark.patch.version").split("-").head.toInt
  lazy val BuildSparkCompatVersionString: String = props.getProperty("spark.compat.version")

  lazy val BuildScalaMajorVersion: Int = props.getProperty("scala.major.version").toInt
  lazy val BuildScalaMinorVersion: Int = props.getProperty("scala.minor.version").toInt
  lazy val BuildScalaPatchVersion: Int = props.getProperty("scala.patch.version").toInt
  lazy val BuildScalaCompatVersionString: String = props.getProperty("scala.compat.version")

  val BuildSparkVersion: (Int, Int, Int) = (BuildSparkMajorVersion, BuildSparkMinorVersion, BuildSparkPatchVersion)
  val BuildSparkCompatVersion: (Int, Int) = (BuildSparkMajorVersion, BuildSparkMinorVersion)

  val BuildScalaVersion: (Int, Int, Int) = (BuildScalaMajorVersion, BuildScalaMinorVersion, BuildScalaPatchVersion)
  val BuildScalaCompatVersion: (Int, Int) = (BuildScalaMajorVersion, BuildScalaMinorVersion)
}
