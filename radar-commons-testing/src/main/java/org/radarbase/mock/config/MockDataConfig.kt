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
package org.radarbase.mock.config

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.avro.specific.SpecificRecord
import org.radarbase.config.AvroTopicConfig
import org.radarbase.topic.AvroTopic
import org.radarcns.kafka.ObservationKey
import java.nio.file.Path
import java.nio.file.Paths

class MockDataConfig : AvroTopicConfig() {
    @JvmField
    @JsonProperty("file")
    var dataFile: String? = null

    @JvmField
    var frequency = 1
    var sensor: String? = null

    @JvmField
    @JsonProperty("value_fields")
    var valueFields: List<String>? = null
    var absoluteDataFile: String? = null
        private set

    @JsonProperty("maximum_difference")
    var maximumDifference = 1e-10

    @JvmField
    var minimum = -1e5

    @JvmField
    var maximum = 1e5

    /**
     * Parse an AvroTopic from the values in this class. If keySchema is not set, ObservationKey
     * will be used as a key schema.
     */
    override fun <K : SpecificRecord, V : SpecificRecord> parseAvroTopic(): AvroTopic<K, V> {
        if (keySchema == null) {
            keySchema = ObservationKey::class.java.getName()
        }
        return super.parseAvroTopic()
    }

    /**
     * Get the data file associated with this definition, relative to given directory.
     * If the data file is specified as an absolute path, then this will return that path.
     *
     * @param root directory the data file is relative to.
     * @return absolute path to the data file
     * @throws NullPointerException if root is null
     */
    fun getDataFile(root: Path): Path {
        val fileName = checkNotNull(dataFile) { "Missing file parameter" }
        val dataPath = Paths.get(fileName)
        return if (dataPath.isAbsolute) {
            dataPath
        } else {
            val absoluteFile = root.resolve(fileName).toAbsolutePath()
            absoluteDataFile = absoluteFile.toString()
            absoluteFile
        }
    }

    fun setValueField(valueField: String) {
        valueFields = listOf(valueField)
    }

    fun setInterval(minimum: Double, maximum: Double) {
        this.minimum = minimum
        this.maximum = maximum
    }

    override fun toString(): String {
        return (
            "MockDataConfig{" +
                "topic='" + topic + '\'' +
                ", valueSchema='" + valueSchema + '\'' +
                ", dataFile='" + dataFile + '\'' +
                ", frequency=" + frequency +
                ", sensor='" + sensor + '\'' +
                ", valueFields=" + valueFields +
                ", maximumDifference=" + maximumDifference +
                ", minimum=" + minimum +
                ", maximum=" + maximum +
                '}'
            )
    }
}
