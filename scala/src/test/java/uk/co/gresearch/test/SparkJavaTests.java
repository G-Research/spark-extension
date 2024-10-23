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

// these tests are deliberately located outside uk.co.gresearch.spark to show how imports look for Java
package uk.co.gresearch.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.gresearch.spark.Backticks;
import uk.co.gresearch.spark.Histogram;
import uk.co.gresearch.spark.RowNumbers;
import uk.co.gresearch.spark.UnpersistHandle;
import uk.co.gresearch.spark.diff.JavaValue;

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

        JavaValue valueOne = new JavaValue(1, "one", 1.0);
        JavaValue valueTwo = new JavaValue(2, "two", 2.0);
        JavaValue valueThree = new JavaValue(3, "three", 3.0);
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
        Dataset<Row> histogram = Histogram.of(dataset, Arrays.asList(0, 1, 2), new Column("id"), new Column("label"));
        List<Row> expected = Arrays.asList(
                RowFactory.create("one", 0, 1, 0, 0),
                RowFactory.create("three", 0, 0, 0, 1),
                RowFactory.create("two", 0, 0, 1, 0)
        );
        Assert.assertEquals(expected, histogram.sort("label").collectAsList());
    }

    @Test
    public void testRowNumbers() {
        Dataset<Row> withRowNumbers = RowNumbers.of(dataset);
        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "one", 1.0, 1),
                RowFactory.create(2, "two", 2.0, 2),
                RowFactory.create(3, "three", 3.0, 3)
        );
        Assert.assertEquals(expected, withRowNumbers.orderBy("id").collectAsList());
    }

    @Test
    public void testRowNumbersOrderOneColumn() {
        Dataset<Row> withRowNumbers = RowNumbers.withOrderColumns(dataset.col("id").desc()).of(dataset);
        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "one", 1.0, 3),
                RowFactory.create(2, "two", 2.0, 2),
                RowFactory.create(3, "three", 3.0, 1)
        );
        Assert.assertEquals(expected, withRowNumbers.orderBy("id").collectAsList());
    }

    @Test
    public void testRowNumbersOrderTwoColumns() {
        Dataset<Row> withRowNumbers = RowNumbers.withOrderColumns(dataset.col("id"), dataset.col("label")).of(dataset);
        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "one", 1.0, 1),
                RowFactory.create(2, "two", 2.0, 2),
                RowFactory.create(3, "three", 3.0, 3)
        );
        Assert.assertEquals(expected, withRowNumbers.orderBy("id").collectAsList());
    }

    @Test
    public void testRowNumbersOrderDesc() {
        Dataset<Row> withRowNumbers = RowNumbers.withOrderColumns(dataset.col("id").desc()).of(dataset);
        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "one", 1.0, 3),
                RowFactory.create(2, "two", 2.0, 2),
                RowFactory.create(3, "three", 3.0, 1)
        );
        Assert.assertEquals(expected, withRowNumbers.orderBy("id").collectAsList());
    }

    @Test
    public void testRowNumbersUnpersist() {
        CacheManager cacheManager = SparkJavaTests.spark.sharedState().cacheManager();
        cacheManager.clearCache();
        Assert.assertTrue(cacheManager.isEmpty());

        UnpersistHandle unpersist = new UnpersistHandle();
        Dataset<Row> withRowNumbers = RowNumbers.withUnpersistHandle(unpersist).of(dataset);
        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "one", 1.0, 1),
                RowFactory.create(2, "two", 2.0, 2),
                RowFactory.create(3, "three", 3.0, 3)
        );
        Assert.assertEquals(expected, withRowNumbers.orderBy("id").collectAsList());

        Assert.assertFalse(cacheManager.isEmpty());
        unpersist.apply(true);
        Assert.assertTrue(cacheManager.isEmpty());
    }

    @Test
    public void testRowNumbersStorageLevelAndUnpersist() {
        CacheManager cacheManager = SparkJavaTests.spark.sharedState().cacheManager();
        cacheManager.clearCache();
        Assert.assertTrue(cacheManager.isEmpty());

        UnpersistHandle unpersist = new UnpersistHandle();
        RowNumbers.withStorageLevel(StorageLevel.MEMORY_ONLY()).withUnpersistHandle(unpersist).of(dataset);

        Assert.assertFalse(cacheManager.isEmpty());
        unpersist.apply(true);
        Assert.assertTrue(cacheManager.isEmpty());
    }

    @Test
    public void testRowNumbersColumnName() {
        Dataset<Row> withRowNumbers = RowNumbers.withRowNumberColumnName("row").of(dataset);
        Assert.assertEquals(Arrays.asList("id", "label", "score", "row"), Arrays.asList(withRowNumbers.columns()));

        List<Row> expected = Arrays.asList(
                RowFactory.create(1, "one", 1.0, 1),
                RowFactory.create(2, "two", 2.0, 2),
                RowFactory.create(3, "three", 3.0, 3)
        );
        Assert.assertEquals(expected, withRowNumbers.orderBy("id").collectAsList());
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

}
