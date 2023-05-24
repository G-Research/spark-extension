/*
 * Copyright 2022 G-Research
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

import java.io.Serializable;
import java.util.Objects;

public class JavaValueAs implements Serializable {
    private String diff;
    private Integer id;
    private String left_label;
    private String right_label;
    private Double left_score;
    private Double right_score;

    public JavaValueAs() { }

    public JavaValueAs(String diff, Integer id, String left_label, String right_label, Double left_score, Double right_score) {
        this.diff = diff;
        this.id = id;
        this.left_label = left_label;
        this.right_label = right_label;
        this.left_score = left_score;
        this.right_score = right_score;
    }

    public String getDiff() {
        return diff;
    }

    public void setDiff(String diff) {
        this.diff = diff;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getLeft_label() {
        return left_label;
    }

    public void setLeft_label(String left_label) {
        this.left_label = left_label;
    }

    public String getRight_label() {
        return right_label;
    }

    public void setRight_label(String right_label) {
        this.right_label = right_label;
    }

    public Double getLeft_score() {
        return left_score;
    }

    public void setLeft_score(Double left_score) {
        this.left_score = left_score;
    }

    public Double getRight_score() {
        return right_score;
    }

    public void setRight_score(Double right_score) {
        this.right_score = right_score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JavaValueAs that = (JavaValueAs) o;
        return Objects.equals(diff, that.diff) && Objects.equals(id, that.id) && Objects.equals(left_label, that.left_label) && Objects.equals(right_label, that.right_label) && Objects.equals(left_score, that.left_score) && Objects.equals(right_score, that.right_score);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diff, id, left_label, right_label, left_score, right_score);
    }

    @Override
    public String toString() {
        return "JavaValueAs{" +
                "diff='" + diff + "', " +
                "id=" + id + ", " +
                "left_label='" + left_label + "', " +
                "right_label='" + right_label + "', " +
                "left_score=" + left_score + ", " +
                "right_score=" + right_score +
                '}';
    }
}
