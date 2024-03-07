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
    id("com.github.davidmc24.gradle.plugin.avro")
    kotlin("plugin.allopen")
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
    implementation(platform("com.fasterxml.jackson:jackson-bom:${Versions.jackson}"))
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${Versions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${Versions.jackson}")

    api("org.apache.avro:avro:${Versions.avro}") {
        implementation("org.apache.commons:commons-compress:${Versions.commonsCompress}")
    }

    implementation("org.apache.kafka:kafka-clients:${Versions.kafka}") {
        implementation("org.xerial.snappy:snappy-java:${Versions.snappy}")
    }

    testImplementation("org.mockito:mockito-core:${Versions.mockito}")
    // Direct producer uses KafkaAvroSerializer if initialized

    implementation("io.confluent:kafka-avro-serializer:${Versions.confluent}") {
        runtimeOnly("com.google.guava:guava:${Versions.guava}")
    }
    testImplementation("org.radarbase:radar-schemas-commons:${Versions.radarSchemas}")
}

allOpen {
    annotation("org.radarbase.config.OpenConfig")
}
