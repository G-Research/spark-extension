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
import org.apache.spark.sql.types.DataTypes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple3;
import scala.math.Equiv;
import uk.co.gresearch.spark.diff.comparator.DiffComparator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.Math.abs;

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

        JavaValue valueOne = new JavaValue(1, "one", 1.0);
        JavaValue valueTwo = new JavaValue(2, "two", 2.0);
        JavaValue valueThree = new JavaValue(3, "three", 3.0);
        JavaValue valueThreeScored = new JavaValue(3, "three", 3.1);
        JavaValue valueFour = new JavaValue(4, "four", 4.0);
        Encoder<JavaValue> encoder = Encoders.bean(JavaValue.class);

        left = spark.createDataset(Arrays.asList(valueOne, valueTwo, valueThree), encoder);
        right = spark.createDataset(Arrays.asList(valueTwo, valueThreeScored, valueFour), encoder);
    }

    @Test
    public void testDiff() {
        Dataset<Row> diff = Diff.of(left.toDF(), right.toDF(), "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null, 1.0, null),
                RowFactory.create("N", 2, "two", "two", 2.0, 2.0),
                RowFactory.create("C", 3, "three", "three", 3.0, 3.1),
                RowFactory.create("I", 4, null, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffNoKey() {
        Dataset<Row> diff = Diff.of(left, right);
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", 1.0),
                RowFactory.create("N", 2, "two", 2.0),
                RowFactory.create("D", 3, "three", 3.0),
                RowFactory.create("I", 3, "three", 3.1),
                RowFactory.create("I", 4, "four", 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id", "diff").collectAsList());
    }

    @Test
    public void testDiffSingleKey() {
        Dataset<Row> diff = Diff.of(left, right, "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null, 1.0, null),
                RowFactory.create("N", 2, "two", "two", 2.0, 2.0),
                RowFactory.create("C", 3, "three", "three", 3.0, 3.1),
                RowFactory.create("I", 4, null, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffMultipleKeys() {
        Dataset<Row> diff = Diff.of(left, right, "id", "label");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", 1.0, null),
                RowFactory.create("N", 2, "two", 2.0, 2.0),
                RowFactory.create("C", 3, "three", 3.0, 3.1),
                RowFactory.create("I", 4, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffIgnoredColumn() {
        Dataset<Row> diff = Diff.of(left, right, Collections.singletonList("id"), Collections.singletonList("score"));
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null, 1.0, null),
                RowFactory.create("N", 2, "two", "two", 2.0, 2.0),
                RowFactory.create("N", 3, "three", "three", 3.0, 3.1),
                RowFactory.create("I", 4, null, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffAs() {
        Encoder<JavaValueAs> encoder = Encoders.bean(JavaValueAs.class);
        Dataset<JavaValueAs> diff = Diff.ofAs(left.toDF(), right.toDF(), encoder, "id");
        List<JavaValueAs> expected = Arrays.asList(
                new JavaValueAs("D", 1, "one", null, 1.0, null),
                new JavaValueAs("N", 2, "two", "two", 2.0, 2.0),
                new JavaValueAs("C", 3, "three", "three", 3.0, 3.1),
                new JavaValueAs("I", 4, null, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffOfWith() {
        Dataset<Tuple3<String, JavaValue, JavaValue>> diff = Diff.ofWith(left, right, "id");
        List<Tuple3<String, JavaValue, JavaValue>> expected = Arrays.asList(
                new Tuple3<>("D", new JavaValue(1, "one", 1.0), null),
                new Tuple3<>("N", new JavaValue(2, "two", 2.0), new JavaValue(2, "two", 2.0)),
                new Tuple3<>("C", new JavaValue(3, "three", 3.0), new JavaValue(3, "three", 3.1)),
                new Tuple3<>("I", null, new JavaValue(4, "four", 4.0))
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDiffer() {
        DiffOptions options = new DiffOptions();

        Differ differ = new Differ(options);
        Dataset<Row> diff = differ.diff(left, right, "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null, 1.0, null),
                RowFactory.create("N", 2, "two", "two", 2.0, 2.0),
                RowFactory.create("C", 3, "three", "three", 3.0, 3.1),
                RowFactory.create("I", 4, null, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
    }

    @Test
    public void testDifferWithIgnored() {
        DiffOptions options = new DiffOptions();

        Differ differ = new Differ(options);
        Dataset<Row> diff = differ.diff(left, right, Collections.singletonList("id"), Collections.singletonList("score"));
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null, 1.0, null),
                RowFactory.create("N", 2, "two", "two", 2.0, 2.0),
                RowFactory.create("N", 3, "three", "three", 3.0, 3.1),
                RowFactory.create("I", 4, null, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
        List<String> columns = Arrays.asList(diff.schema().fieldNames());
        Assert.assertEquals(Arrays.asList("diff", "id", "left_label", "right_label", "left_score", "right_score"), columns);
    }

    @Test
    public void testDiffWithOptions() {
        DiffOptions options = new DiffOptions(
                "action",
                "before", "after",
                "+", "~", "-", "=",
                scala.Option.apply(null),
                DiffMode.ColumnByColumn(),
                false
        );

        Differ differ = new Differ(options);
        Dataset<Row> diff = differ.diff(left, right, "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("-", 1, "one", null, 1.0, null),
                RowFactory.create("=", 2, "two", "two", 2.0, 2.0),
                RowFactory.create("~", 3, "three", "three", 3.0, 3.1),
                RowFactory.create("+", 4, null, "four", null, 4.0)
        );
        Assert.assertEquals(expected, diff.sort("id").collectAsList());
        List<String> names = Arrays.asList(diff.schema().fieldNames());
        Assert.assertEquals(Arrays.asList("action", "id", "before_label", "after_label", "before_score", "after_score"), names);
    }

    @Test
    public void testDiffWithComparators() {
        DiffComparator comparator = DiffComparators.epsilon(0.100000001).asInclusive().asAbsolute();
        testDiffWithComparator(new DiffOptions().withComparator(comparator, DataTypes.DoubleType));
        testDiffWithComparator(new DiffOptions().withComparator(comparator, "score"));

        Equiv<Double> equivDouble = (Double x, Double y) -> x == null && y == null || x != null && y != null &&
                abs(x - y) <= 0.1000000001;
        testDiffWithComparator(new DiffOptions().withComparator(equivDouble, Encoders.DOUBLE()));
        testDiffWithComparator(new DiffOptions().withComparator(equivDouble, Encoders.DOUBLE(), "score"));

        Equiv<Object> equivAny = (x, y) -> x == null && y == null || x instanceof Double && y instanceof Double &&
                abs((Double) x - (Double) y) <= 0.1000000001;
        testDiffWithComparator(new DiffOptions().withComparator(equivAny, DataTypes.DoubleType));
        testDiffWithComparator(new DiffOptions().withComparator(equivAny, "score"));
    }

    private void testDiffWithComparator(DiffOptions options) {
        Differ differ = new Differ(options);

        Dataset<Row> diff = differ.diff(left, right, "id");
        List<Row> expected = Arrays.asList(
                RowFactory.create("D", 1, "one", null, 1.0, null),
                RowFactory.create("N", 2, "two", "two", 2.0, 2.0),
                // this is only considered un-changed because of the epsilon diff comparator for column 'score'
                RowFactory.create("N", 3, "three", "three", 3.0, 3.1),
                RowFactory.create("I", 4, null, "four", null, 4.0)
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
