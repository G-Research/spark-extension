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

import org.apache.parquet.crypto.ParquetCryptoRuntimeException
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData}
import org.apache.parquet.schema.PrimitiveType

import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

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

/**
 * Guard access to possibly encrypted and inaccessible metadata of a footer.
 *   - If footer is encrypted while we have no decryption keys, metadata values are None.
 *   - If footer is known not to be encrypted, metadata values are Some.
 *   - If we don't know whether the footer is encrypted, we access some metadata that we could not read if encrypted to
 *     determine the encryption state of the footer.
 */
private case class FooterGuard(footer: Footer) {
  lazy val isSafe: Boolean = {
    // having a decryptor tells us this file is expected to be decryptable
    Option(footer.getParquetMetadata.getFileMetaData.getFileDecryptor)
      // otherwise, when we have an unencrypted file, we are also safe to access f
      .orElse(
        ParquetMetaDataUtil
          .getEncryptionType(footer.getParquetMetadata.getFileMetaData)
          .filter(_ == "UNENCRYPTED")
      )
      // turn to Some(true) if safe, None if unknown
      .map(_ => true)
      // otherwise, we access some metadata that if the footer is encrypted would fail
      .orElse(
        Some(
          Try(footer.getParquetMetadata.getBlocks.headOption.map(_.getTotalByteSize))
            // get hold of the possible exception
            .toEither.swap.toOption
            // no exception means safe, ignore exceptions other than ParquetCryptoRuntimeException
            .exists(!_.isInstanceOf[ParquetCryptoRuntimeException])
        )
      )
      // now is Some(true) or Some(false)
      .get
  }

  private[parquet] def apply[T](f: => T): Option[T] = {
    if (isSafe) { Some(f) }
    else { None }
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
