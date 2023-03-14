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
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("com.github.davidmc24.gradle.plugin.avro")
}

description = "RADAR Common server library utilities."

dependencies {
    api(project(":radar-commons"))

    // For POJO classes and ConfigLoader
    val jacksonVersion: String by project
    implementation(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.fasterxml.jackson.core:jackson-databind")

    val avroVersion: String by project
    api("org.apache.avro:avro:$avroVersion")

    val kafkaVersion: String by project
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    val mockitoVersion: String by project
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    // Direct producer uses KafkaAvroSerializer if initialized
    val confluentVersion: String by project
    testImplementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    val radarSchemasVersion: String by project
    testImplementation("org.radarbase:radar-schemas-commons:$radarSchemasVersion")
    val slf4jVersion: String by project
    testRuntimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")
}

tasks.withType<JavaCompile> {
    dependsOn(tasks.named("generateAvroJava"))
}

tasks.withType<KotlinCompile> {
    dependsOn(tasks.named("generateAvroJava"))
}
