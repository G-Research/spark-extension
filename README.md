# Spark Extension

This project provides extensions to the [Apache Spark project](https://spark.apache.org/):
- [Diff](DIFF.md): A `diff` transformation for `Dataset`s that computes the differences between
two datasets, i.e. which rows to _add_, _delete_ or _change_ to get from one dataset to the other.

## Using Spark Extension

### SBT

Add this line to your `build.sbt` file:

```sbt
libraryDependencies += "uk.co.gresearch.spark" %% "spark-extension" % "1.0.0"
```

### Maven

Add this dependency to your `pom.xml` file:

```xml
<dependency>
  <groupId>uk.co.gresearch.spark</groupId>
  <artifactId>spark-extension_2.12</artifactId>
  <version>1.0.0</version>
</dependency>
```
