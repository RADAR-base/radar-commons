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
    plugins {
        val kotlinVersion: String by settings
        kotlin("jvm") version kotlinVersion
        kotlin("plugin.serialization") version kotlinVersion

        val avroPluginVersion: String by settings
        id("com.github.davidmc24.gradle.plugin.avro") version avroPluginVersion
        val nexusPluginVersion: String by settings
        id("io.github.gradle-nexus.publish-plugin") version nexusPluginVersion

        val dependencyUpdatePluginVersion: String by settings
        id("com.github.ben-manes.versions") version dependencyUpdatePluginVersion
        val dokkaVersion: String by settings
        id("org.jetbrains.dokka") version dokkaVersion
    }
}
