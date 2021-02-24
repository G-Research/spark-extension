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

package uk.co.gresearch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.gresearch.spark.diff.JavaValue;
import org.apache.spark.sql.Column;

import java.util.Arrays;
import java.util.List;

public class SparkJavaTests {
    private static SparkSession spark;
    private static Dataset<JavaValue> dataset;

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
        dataset = spark.createDataset(Arrays.asList(valueOne, valueTwo, valueThree), encoder);
    }

    @Test
    public void testBackticks() {
        Assert.assertEquals("col", Backticks.column_name("col"));
        Assert.assertEquals("`a.col`", Backticks.column_name("a.col"));
        Assert.assertEquals("a.col", Backticks.column_name("a", "col"));
        Assert.assertEquals("some.more.columns", Backticks.column_name("some", "more", "columns"));
        Assert.assertEquals("some.`more.columns`", Backticks.column_name("some", "more.columns"));
        Assert.assertEquals("some.more.dotted.columns", Backticks.column_name("some", "more", "dotted", "columns"));
        Assert.assertEquals("some.more.`dotted.columns`", Backticks.column_name("some", "more", "dotted.columns"));
    }

    @Test
    public void testHistogram() {
        Dataset<Row> histogram = Histogram.of(dataset, Arrays.asList(0, 1, 2), new Column("id"));
        List<Row> expected = Arrays.asList(RowFactory.create(0, 1, 1, 1));
        Assert.assertEquals(expected, histogram.collectAsList());
    }

    @Test
    public void testHistogramWithAggColumn() {
        Dataset<Row> histogram = Histogram.of(dataset, Arrays.asList(0, 1, 2), new Column("id"), new Column("value"));
        List<Row> expected = Arrays.asList(
                RowFactory.create("one", 0, 1, 0, 0),
                RowFactory.create("three", 0, 0, 0, 1),
                RowFactory.create("two", 0, 0, 1, 0)
        );
        Assert.assertEquals(expected, histogram.sort("value").collectAsList());
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

}
