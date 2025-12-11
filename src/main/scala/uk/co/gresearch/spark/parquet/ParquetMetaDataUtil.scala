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

package uk.co.gresearch.spark.parquet

import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData}
import org.apache.parquet.schema.PrimitiveType

import scala.reflect.{ClassTag, classTag}
import scala.util.Try

private trait MethodGuard {
  def isSupported[T: ClassTag](methodName: String): Boolean = {
    Try(classTag[T].runtimeClass.getMethod(methodName)).isSuccess
  }

  def guard[T, R](supported: Boolean)(f: T => R): T => Option[R] =
    guardOption(supported)(t => Some(f(t)))

  def guardOption[T, R](supported: Boolean)(f: T => Option[R]): T => Option[R] =
    if (supported) { (v: T) =>
      f(v)
    } else { (_: T) =>
      None
    }
}

private[parquet] object ParquetMetaDataUtil extends MethodGuard {
  lazy val getEncryptionTypeIsSupported: Boolean =
    isSupported[FileMetaData]("getEncryptionType")
  lazy val getEncryptionType: FileMetaData => Option[String] =
    guard(getEncryptionTypeIsSupported) { fileMetaData: FileMetaData =>
      fileMetaData.getEncryptionType.name()
    }

  lazy val getLogicalTypeAnnotationIsSupported: Boolean =
    isSupported[PrimitiveType]("getLogicalTypeAnnotation")
  lazy val getLogicalTypeAnnotation: PrimitiveType => Option[String] =
    guardOption(getLogicalTypeAnnotationIsSupported) { (primitive: PrimitiveType) =>
      Option(primitive.getLogicalTypeAnnotation).map(_.toString)
    }

  lazy val getOrdinalIsSupported: Boolean =
    isSupported[BlockMetaData]("getOrdinal")
  lazy val getOrdinal: BlockMetaData => Option[Int] =
    guard(getOrdinalIsSupported) { (block: BlockMetaData) =>
      block.getOrdinal
    }
}
