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

package uk.co.gresearch

import java.io.File

package object spark {

  protected def withTempPath[T](f: File => T): T = {
    val dir = File.createTempFile("test", ".tmp")
    dir.delete()
    try f(dir) finally delete(dir)
  }

  private def delete(file: File): Unit =
    if(file.isDirectory)
      file.listFiles().toSeq.foreach(delete)
    else {
      println(file)
//      file.delete()
    }

}
