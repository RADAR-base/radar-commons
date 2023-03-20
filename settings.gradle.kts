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

include(":radar-commons")
include(":radar-commons-kotlin")
include(":radar-commons-testing")
include(":radar-commons-server")

pluginManagement {
    val kotlin = "1.8.10"
    val avro = "1.6.0"
    val dependencyUpdate = "0.46.0"
    val nexus = "1.3.0"
    val dokka = "1.8.10"

    plugins {
        kotlin("plugin.serialization") version kotlin
        id("com.github.davidmc24.gradle.plugin.avro") version avro
    }
}
