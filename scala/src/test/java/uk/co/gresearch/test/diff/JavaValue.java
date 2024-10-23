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

import java.io.Serializable;
import java.util.Objects;

public class JavaValue implements Serializable {
    private Integer id;
    private String label;
    private Double score;

    public JavaValue() { }

    public JavaValue(Integer id, String label, Double score) {
        this.id = id;
        this.label = label;
        this.score = score;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JavaValue javaValue = (JavaValue) o;
        return Objects.equals(id, javaValue.id) && Objects.equals(label, javaValue.label) && Objects.equals(score, javaValue.score);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, label, score);
    }

    @Override
    public String toString() {
        return "JavaValue{id=" + id + ", label='" + label + "', score=" + score + '}';
    }
}
