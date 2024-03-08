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
    application
}

val applicationRuntimeOnly: Configuration by configurations.creating

application {
    mainClass.set("org.radarbase.mock.MockProducer")
}

tasks.named<JavaExec>("run") {
    classpath += applicationRuntimeOnly
    if (project.hasProperty("mockConfig")) {
        args(project.property("mockConfig"))
    } else {
        args("mock.yml")
    }
}

description = "RADAR Common testing library mocking code and utilities."

dependencies {
    api(project(":radar-commons"))
    api(project(":radar-commons-server"))
    api(project(":radar-commons-kotlin"))

    api("org.radarbase:radar-schemas-commons:${Versions.radarSchemas}")

    implementation("com.opencsv:opencsv:${Versions.opencsv}")
    implementation(platform("com.fasterxml.jackson:jackson-bom:${Versions.jackson}"))
    implementation("com.fasterxml.jackson.core:jackson-databind:${Versions.jackson}")

    implementation("org.apache.kafka:kafka-clients:${Versions.kafka}") {
        implementation("org.xerial.snappy:snappy-java:${Versions.snappy}")
    }

    implementation("io.confluent:kafka-avro-serializer:${Versions.confluent}") {
        runtimeOnly("com.google.guava:guava:${Versions.guava}")
    }

    api("org.apache.avro:avro:${Versions.avro}") {
        implementation("org.apache.commons:commons-compress:${Versions.commonsCompress}")
    }

    implementation(platform("io.ktor:ktor-bom:${Versions.ktor}"))
    implementation("io.ktor:ktor-serialization-kotlinx-json:${Versions.ktor}")

    applicationRuntimeOnly("org.slf4j:slf4j-simple:${Versions.slf4j}")

    testImplementation("org.hamcrest:hamcrest:${Versions.hamcrest}")
    testImplementation("org.mockito:mockito-core:${Versions.mockito}")
}
