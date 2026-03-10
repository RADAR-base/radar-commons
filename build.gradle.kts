import org.radarbase.gradle.plugin.radarKotlin
import org.radarbase.gradle.plugin.radarPublishing

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
    id("org.radarbase.radar-root-project")
    id("org.radarbase.radar-dependency-management")
    alias(libs.plugins.dokka)
    alias(libs.plugins.dokka.javadoc)
}

val githubRepoName = "RADAR-base/radar-commons"
val githubUrl = "https://github.com/$githubRepoName"

radarRootProject {
    projectVersion.set(libs.versions.project)
    gradleVersion.set(libs.versions.gradle)
}

repositories {
    mavenCentral()
}

subprojects {

    // --- Vulnerability fixes start ---
    dependencies {
        plugins.withType<JavaPlugin> {
            constraints {
                add("implementation", rootProject.libs.jackson.bom) {
                    because("Force safe version of Jackson across all modules")
                }
            }
        }
    }

    configurations.all {
        resolutionStrategy.dependencySubstitution {
            // Substitute the old group/module with the new one
            substitute(module("org.lz4:lz4-java"))
                .using(module(rootProject.libs.lz4.get().toString()))
                .because("Force safe version of LZ4 across all modules")
        }
    }
    // --- Vulnerability fixes end ---

    apply(plugin = "org.radarbase.radar-kotlin")
    apply(plugin = "org.radarbase.radar-publishing")
    radarPublishing {
        githubUrl.set("https://github.com/$githubRepoName")
        developers {
            developer {
                id.set("pvannierop")
                email.set("pim@thehyve.nl")
                name.set("Pim van Nierop")
                organization.set("The Hyve")
            }
        }
    }

    radarKotlin {
        javaVersion.set(rootProject.libs.versions.java.get().toInt())
        kotlinVersion.set(rootProject.libs.versions.kotlin)
        junitVersion.set(rootProject.libs.versions.junit)
        slf4jVersion.set(rootProject.libs.versions.slf4j)
    }

    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "org.jetbrains.dokka-javadoc")

    dokka {
        dokkaSourceSets.configureEach {
            externalDocumentationLinks.register("avro") {
                val baseUrl = "https://javadoc.io/doc/org.apache.avro/avro/${libs.versions.avro.get()}/"
                url.set(project.uri(baseUrl))
                packageListUrl.set(project.uri("${baseUrl}element-list"))
            }
            externalDocumentationLinks.register("kotlinx.coroutines") {
                val baseUrl = "https://kotlinlang.org/api/kotlinx.coroutines/"
                url.set(project.uri(baseUrl))
                packageListUrl.set(project.uri("${baseUrl}package-list"))
            }
            externalDocumentationLinks.register("ktor") {
                val baseUrl = "https://api.ktor.io/"
                url.set(project.uri(baseUrl))
                packageListUrl.set(project.uri("${baseUrl}package-list"))
            }
            externalDocumentationLinks.register("java") {
                url.set(project.uri("https://docs.oracle.com/en/java/javase/17/docs/api/"))
                packageListUrl.set(project.uri("https://docs.oracle.com/en/java/javase/17/docs/api/element-list"))
            }
        }
    }
}
