/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.diff

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper

trait SparkTestSession extends SQLHelper {

  lazy val spark:
    SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
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
    super.withSQLConf(pairs:_*)(f)
  }
}


case class Empty()
case class Value(id: Int, value: Option[String])
case class Value2(id: Int, seq: Option[Int], value: Option[String])
case class Value3(id: Int, left_value: String, right_value: String, value: String)
case class Value4(id: Int, diff: String)
case class Value5(first_id: Int, id: String)
case class Value6(id: Int, label: String)

case class DiffAs(diff: String,
                  id: Int,
                  left_value: Option[String],
                  right_value: Option[String])
case class DiffAsCustom(action: String,
                        id: Int,
                        before_value: Option[String],
                        after_value: Option[String])
case class DiffAsSubset(diff: String,
                        id: Int,
                        left_value: Option[String])
case class DiffAsExtra(diff: String,
                       id: Int,
                       left_value: Option[String],
                       right_value: Option[String],
                       extra: String)
