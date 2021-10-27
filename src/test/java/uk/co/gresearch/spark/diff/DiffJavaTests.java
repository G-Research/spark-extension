/*
 * Copyright 2021 G-Research
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

package uk.co.gresearch.spark.diff;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DiffJavaTests {
    private static SparkSession spark;
    private static Dataset<JavaValue> left;
    private static Dataset<JavaValue> right;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession
                    .builder()
                    .master("local[*]")
                    .config(new SparkConf().set("fs.defaultFS", "file:///"))
                    .appName("Diff Java Suite")
                    .getOrCreate();

        JavaValue valueOne = new JavaValue(1, "one");
        JavaValue valueTwo = new JavaValue(2, "two");
        JavaValue valueThree = new JavaValue(3, "three");
        Encoder<JavaValue> encoder = Encoders.bean(JavaValue.class);

        left = spark.createDataset(Arrays.asList(valueOne, valueTwo), encoder);
        right = spark.createDataset(Arrays.asList(valueTwo, valueThree), encoder);
    }

    @Test
    public void testDiffDataFrame() {
        Dataset<Row> diff = Diff.of(left.toDF(), right.toDF(), "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null),
                RowFactory.create("N", 2, "two", "two"),
                RowFactory.create("I", 3, null, "three")
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffNoKey() {
        Dataset<Row> diff = Diff.of(left, right);
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one"),
                RowFactory.create("N", 2, "two"),
                RowFactory.create("I", 3, "three")
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffSingleKey() {
        Dataset<Row> diff = Diff.of(left, right, "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null),
                RowFactory.create("N", 2, "two", "two"),
                RowFactory.create("I", 3, null, "three")
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffMultipleKeys() {
        Dataset<Row> diff = Diff.of(left, right, "id", "value");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one"),
                RowFactory.create("N", 2, "two"),
                RowFactory.create("I", 3, "three")
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffWithOptions() {
        DiffOptions options = new DiffOptions(
                "diff",
                "left", "right",
                "I", "C", "D", "N",
                scala.Option.apply(null),
                DiffMode.ColumnByColumn(),
                false
        );

        Differ differ = new Differ(options);
        Dataset<Row> diff = differ.diff(left, right, "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null),
                RowFactory.create("N", 2, "two", "two"),
                RowFactory.create("I", 3, null, "three")
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }
}
