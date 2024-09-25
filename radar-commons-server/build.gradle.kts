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
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.fasterxml.jackson.core:jackson-databind")

    api(libs.avro) {
        implementation(libs.commons.compress)
    }

    // Somehow, using version catalog for kafka-clients does not work (throws task error).
    implementation("org.apache.kafka:kafka-clients:${Versions.kafka}") {
        implementation(libs.snappy.java)
    }

    testImplementation(libs.mockito.core)

    // Direct producer uses KafkaAvroSerializer if initialized
    implementation(libs.kafka.avro.serializer) {
        runtimeOnly(libs.guava)
    }
    testImplementation(libs.radar.schemas.commons)
}

allOpen {
    annotation("org.radarbase.config.OpenConfig")
}
