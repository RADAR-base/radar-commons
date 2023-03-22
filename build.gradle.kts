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
import org.radarbase.gradle.plugin.radarKotlin
import org.radarbase.gradle.plugin.radarPublishing
import org.radarbase.gradle.plugin.radarRootProject

plugins {
    kotlin("plugin.serialization") version Versions.Plugins.kotlinSerialization apply false
    id("com.github.davidmc24.gradle.plugin.avro") version Versions.Plugins.avro apply false
    id("org.radarbase.radar-root-project")
    id("org.radarbase.radar-dependency-management")
}

val githubRepoName = "RADAR-base/radar-commons"
val githubUrl = "https://github.com/$githubRepoName"

radarRootProject {
    projectVersion.set(Versions.project)
}

subprojects {
    // Apply the plugins
    apply(plugin = "org.radarbase.radar-kotlin")
    apply(plugin = "org.radarbase.radar-publishing")

    dependencies {
        configurations["testImplementation"]("org.jetbrains.kotlinx:kotlinx-coroutines-test:${Versions.coroutines}")
    }

    radarPublishing {
        githubUrl.set("https://github.com/$githubRepoName")
        developers {
            developer {
                id.set("blootsvoets")
                name.set("Joris Borgdorff")
                email.set("joris@thehyve.nl")
                organization.set("The Hyve")
            }
            developer {
                id.set("nivemaham")
                name.set("Nivethika Mahasivam")
                email.set("nivethika@thehyve.nl")
                organization.set("The Hyve")
            }
        }
    }

    radarKotlin {
        javaVersion.set(Versions.java)
        kotlinVersion.set(Versions.Plugins.kotlin)
        junitVersion.set(Versions.junit)
    }

    //---------------------------------------------------------------------------//
    // Style checking                                                            //
    //---------------------------------------------------------------------------//

    tasks.withType<Test> {
        useJUnitPlatform()

        val numberOfLines = 100
        val stdout = ArrayDeque<String>(numberOfLines)
        beforeTest(closureOf<TestDescriptor> {
            stdout.clear()
        })

        onOutput(KotlinClosure2<TestDescriptor, TestOutputEvent, Unit>({ _, toe ->
            toe.message.split("(?m)$").forEach { line ->
                if (stdout.size == numberOfLines) {
                    stdout.removeFirst()
                }
                stdout.addLast(line)
            }
        }))

        afterTest(KotlinClosure2<TestDescriptor, TestResult, Unit>({ td, tr ->
            if (tr.resultType == TestResult.ResultType.FAILURE) {
                println()
                print("${td.className}.${td.name} FAILED")
                if (stdout.isEmpty()) {
                    println(" without any output")
                } else {
                    println(" with last 100 lines of output:")
                    println("=".repeat(100))
                    stdout.forEach { print(it) }
                    println("=".repeat(100))
                }
            }
        }))

        testLogging {
            showExceptions = true
            showCauses = true
            showStackTraces = true
            setExceptionFormat("full")
        }
    }
}
