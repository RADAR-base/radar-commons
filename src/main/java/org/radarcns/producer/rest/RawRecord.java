/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.producer.rest;

import com.fasterxml.jackson.annotation.JsonRawValue;

/**
 * Structure of a single Kafka REST request record
 */
public class RawRecord {
    @JsonRawValue
    public String key;
    // Use a raw value, so we can put JSON in this String.
    @JsonRawValue
    public String value;
    public RawRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
