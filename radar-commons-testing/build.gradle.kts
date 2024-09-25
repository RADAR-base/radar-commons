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

    api(libs.radar.schemas.commons)

    implementation(libs.opencsv)
    implementation(platform(libs.jackson.bom))
    implementation("com.fasterxml.jackson.core:jackson-databind")

    // Somehow, using version catalog for kafka-clients does not work (throws task error).
    implementation("org.apache.kafka:kafka-clients:${Versions.kafka}") {
        implementation(libs.snappy.java)
    }
    implementation(libs.kafka.avro.serializer)

    api(libs.avro) {
        implementation(libs.commons.compress)
    }

    implementation(platform(libs.ktor.bom))
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    applicationRuntimeOnly(libs.slf4j.simple)

    testImplementation(libs.hamcrest)
    testImplementation(libs.mockito.core)
}
