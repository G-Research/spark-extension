/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkTestSession extends SQLHelper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[2]")
      .appName("spark test example")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.local.dir", ".")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext

  lazy val sql: SQLContext = spark.sqlContext

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   */
  protected override def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    SparkSession.setActiveSession(spark)
    super.withSQLConf(pairs: _*)(f)
  }

}
