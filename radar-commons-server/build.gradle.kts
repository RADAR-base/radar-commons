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

plugins {
    alias(libs.plugins.kotlin.allopen)
    alias(libs.plugins.avro)
}

description = "RADAR Common server library utilities."

val generateAvroJava by tasks

sourceSets {
    main {
        java {
            srcDirs(generateAvroJava.outputs)
        }
    }
}

dependencies {
    api(project(":radar-commons"))

    // For POJO classes and ConfigLoader
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.jackson.databind)

    api(libs.apache.avro) {
        implementation(libs.apache.commons.compress)
    }

    implementation(libs.kafka.avro.serializer) {
        runtimeOnly(libs.google.guava)
    }

    testImplementation(libs.mockito.core)
    testImplementation(libs.radar.schemas.commons)
    testImplementation(libs.kotlinx.coroutines.test)
    testRuntimeOnly(libs.slf4j.simple)
}

allOpen {
    annotation("org.radarbase.config.OpenConfig")
}
