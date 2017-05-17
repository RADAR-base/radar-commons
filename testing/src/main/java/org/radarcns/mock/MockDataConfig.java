/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.mock;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.AvroTopic;

public class MockDataConfig {
    private String topic;
    @JsonProperty("key_schema")
    private String keySchema;
    @JsonProperty("value_schema")
    private String valueSchema;
    @JsonProperty("file")
    private String dataFile;

    private int frequency = 1;

    private String sensor;

    @JsonProperty("value_fields")
    private List<String> valueFields;

    private String absolutePath;

    @JsonProperty("maximum_difference")
    private double maximumDifference = 1e-10d;

    private double minimum = Double.NEGATIVE_INFINITY;
    private double maximum = Double.POSITIVE_INFINITY;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(String keySchema) {
        this.keySchema = keySchema;
    }

    public String getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(String valueSchema) {
        this.valueSchema = valueSchema;
    }

    @SuppressWarnings("unchecked")
    public AvroTopic<? extends SpecificRecord, ? extends SpecificRecord> parseAvroTopic()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {

        Class<? extends SpecificRecord> keyClass;
        Schema keyAvroSchema;

        if (this.keySchema == null) {
            keyClass = MeasurementKey.class;
            keyAvroSchema = MeasurementKey.getClassSchema();
        } else {
            keyClass = (Class<? extends SpecificRecord>) Class.forName(this.keySchema);
            keyAvroSchema = (Schema) keyClass
                    .getMethod("getClassSchema").invoke(null);
            // check instantiation
            SpecificData.newInstance(keyClass, keyAvroSchema);
        }

        Class<? extends SpecificRecord> valueClass = (Class<? extends SpecificRecord>)
                Class.forName(this.valueSchema);
        Schema valueAvroSchema = (Schema) valueClass
                .getMethod("getClassSchema").invoke(null);
        // check instantiation
        SpecificData.newInstance(valueClass, valueAvroSchema);

        return new AvroTopic<>(topic, keyAvroSchema, valueAvroSchema, keyClass, valueClass);
    }

    public File getDataFile(File configFile) {
        File directDataFile = new File(dataFile);
        if (directDataFile.isAbsolute()) {
            return directDataFile;
        } else {
            File absoluteFile = new File(configFile.getParentFile(), dataFile);
            this.absolutePath = absoluteFile.getAbsolutePath();
            return absoluteFile;
        }
    }

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
    }

    public String getSensor() {
        return sensor;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }

    public String getAbsoluteDataFile() {
        return this.absolutePath;
    }

    public List<String> getValueFields() {
        return valueFields;
    }

    public void setValueFields(List<String> valueFields) {
        this.valueFields = valueFields;
    }

    public void setValueField(String valueField) {
        setValueFields(Collections.singletonList(valueField));
    }

    public double getMaximumDifference() {
        return maximumDifference;
    }

    public void setMaximumDifference(double maximumDifference) {
        this.maximumDifference = maximumDifference;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public double getMinimum() {
        return minimum;
    }

    public void setMinimum(double minimum) {
        this.minimum = minimum;
    }

    public void setInterval(double minimum, double maximum) {
        this.minimum = minimum;
        this.maximum = maximum;
    }

    public double getMaximum() {
        return maximum;
    }

    public void setMaximum(double maximum) {
        this.maximum = maximum;
    }

    public int getFrequency() {
        return frequency;
    }
}
