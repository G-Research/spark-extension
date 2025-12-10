/*
 * Copyright 2025 G-Research
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

package uk.co.gresearch.test

import scala.reflect.io.Path
import uk.co.gresearch.spark.BuildVersion

class ClasspathSuite extends Spec with BuildVersion {
  describe("The classpath") {
    val classpath = System.getProperty("java.class.path").split(":").filter(_.contains("target"))

    val resourceUrl = getClass.getResource("/log4j2.properties")
    val testClasses = Path(resourceUrl.getPath).parent

    it("should contain compiled test classes") {
      assert(classpath.contains(testClasses.path))
    }

    val isIntegrationTest = System.getenv().getOrDefault("CI_INTEGRATION_TEST", "false") == "true"
    val jarFilename = s"spark-extension_$BuildScalaCompatVersionString-$VersionString.jar"
    val jar = testClasses.parent.resolve(Path(jarFilename)).path
    val classes = testClasses.parent.resolve(Path("classes")).path

    it("should contain compiled classes but not the jar") {
      assume(!isIntegrationTest)

      // unit testing does not require the jar to be in the classpath
      assert(!classpath.contains(jar))
      // but the path to the compiled classes
      assert(classpath.contains(classes))
    }

    it("should contain the jar but not compiled classes") {
      assume(isIntegrationTest)

      // integration testing requires the jar to be in the classpath
      assert(classpath.contains(jar))
      // but not the path to the compiled classes
      assert(!classpath.contains(classes))
    }
  }
}
