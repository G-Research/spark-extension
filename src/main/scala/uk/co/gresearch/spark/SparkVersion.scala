/*
 * Copyright 2023 G-Research
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

import org.apache.spark.SPARK_VERSION_SHORT

/**
 * Provides versions form runtime environment.
 */
trait SparkVersion {
  private def SparkVersionSeq: Seq[Int] = SPARK_VERSION_SHORT.split('.').toSeq.map(_.toInt)

  def SparkMajorVersion: Int = SparkVersionSeq.head
  def SparkMinorVersion: Int = SparkVersionSeq(1)
  def SparkPatchVersion: Int = SparkVersionSeq(2)

  def SparkVersion: (Int, Int, Int) = (SparkMajorVersion, SparkMinorVersion, SparkPatchVersion)
  def SparkCompatVersion: (Int, Int) = (SparkMajorVersion, SparkMinorVersion)
  def SparkCompatVersionString: String = SparkVersionSeq.slice(0, 2).mkString(".")
}
