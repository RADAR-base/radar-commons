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
    val avroVersion: String by project
    api("org.apache.avro:avro:$avroVersion")
    val radarSchemasVersion: String by project
    api("org.radarbase:radar-schemas-commons:$radarSchemasVersion")

    val opencsvVersion: String by project
    implementation("com.opencsv:opencsv:$opencsvVersion")
    val jacksonVersion: String by project
    implementation(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    val kafkaVersion: String by project
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    val confluentVersion: String by project
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")

    val ktorVersion: String by project
    implementation(platform("io.ktor:ktor-bom:$ktorVersion"))
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    val slf4jVersion: String by project
    applicationRuntimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")

    val hamcrestVersion: String by project
    testImplementation("org.hamcrest:hamcrest:$hamcrestVersion")
    testImplementation("org.slf4j:slf4j-simple:$slf4jVersion")
    val mockitoVersion: String by project
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
}
