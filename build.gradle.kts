import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import java.util.LinkedList
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

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
    kotlin("jvm") version "1.7.21" apply false
    id("com.github.davidmc24.gradle.plugin.avro") version "1.5.0" apply false
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("com.github.ben-manes.versions") version "0.44.0"
    id("org.jetbrains.dokka") version "1.7.20" apply false
    idea
    `maven-publish`
    signing
}

val githubRepoName = "RADAR-base/radar-commons"
val githubUrl = "https://github.com/$githubRepoName"
val githubIssueUrl = "https://github.com/$githubRepoName/issues"
val website = "https://radar-base.org"

allprojects {
    version = "0.15.1-SNAPSHOT"
    group = "org.radarbase"
}

subprojects {
    val myProject = this

    // Apply the plugins
    apply(plugin = "java")
    apply(plugin = "java-library")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "idea")

    tasks.withType<JavaCompile> {
        targetCompatibility = JavaVersion.VERSION_11.toString()
        sourceCompatibility = JavaVersion.VERSION_11.toString()
    }
    tasks.withType<KotlinCompile> {
        kotlinOptions {
            jvmTarget = "11"
            languageVersion = "1.7"
            apiVersion = "1.7"
        }
    }

    //---------------------------------------------------------------------------//
    // Dependencies                                                              //
    //---------------------------------------------------------------------------//
    repositories {
        mavenCentral()
        mavenLocal()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://oss.sonatype.org/content/repositories/snapshots")
    }

    afterEvaluate {
        configurations {
            named("implementation") {
                resolutionStrategy.cacheChangingModulesFor(0, "SECONDS")
            }
        }
    }

    idea {
        module {
            isDownloadSources = true
        }
    }

    val sourcesJar by tasks.registering(Jar::class) {
        from(myProject.the<SourceSetContainer>()["main"].allSource)
        archiveClassifier.set("sources")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        val classes by tasks
        dependsOn(classes)
    }

    apply(plugin = "org.jetbrains.dokka")
    val dokkaJar by tasks.registering(Jar::class) {
        from("$buildDir/dokka/javadoc")
        archiveClassifier.set("javadoc")
        val dokkaJavadoc by tasks
        dependsOn(dokkaJavadoc)
    }

    tasks.withType<Tar> {
        compression = Compression.GZIP
        archiveExtension.set("tar.gz")
    }

    tasks.withType<Jar> {
        manifest {
            attributes(
                "Implementation-Title" to project.name,
                "Implementation-Version" to project.version
            )
        }
    }

    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    val assemble by tasks
    assemble.dependsOn(sourcesJar)
    assemble.dependsOn(dokkaJar)

    val mavenJar by publishing.publications.creating(MavenPublication::class) {
        from(components["java"])

        artifact(sourcesJar)
        artifact(dokkaJar)

        afterEvaluate {
            pom {
                name.set(myProject.name)
                description.set(myProject.description)
                url.set(githubUrl)
                licenses {
                    license {
                        name.set("The Apache Software License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }
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
                issueManagement {
                    system.set("GitHub")
                    url.set(githubIssueUrl)
                }
                organization {
                    name.set("RADAR-base")
                    url.set("https://radar-base.org")
                }
                scm {
                    connection.set("scm:git:$githubUrl")
                    url.set(githubUrl)
                }
            }
        }
    }

    signing {
        useGpgCmd()
        isRequired = true
        sign(tasks["sourcesJar"], tasks["dokkaJar"])
        sign(mavenJar)
    }

    tasks.withType<Sign> {
        onlyIf { gradle.taskGraph.hasTask(myProject.tasks["publish"]) }
    }

    //---------------------------------------------------------------------------//
    // Style checking                                                            //
    //---------------------------------------------------------------------------//

    tasks.withType<Test> {
        val stdout = LinkedList<String>()
        beforeTest(closureOf<TestDescriptor> {
            stdout.clear()
        })

        onOutput(KotlinClosure2<TestDescriptor, TestOutputEvent, Unit>({ td, toe ->
            stdout.addAll(toe.getMessage().split("(?m)$").toList())
            while (stdout.size > 100) {
                stdout.remove()
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

tasks.withType<DependencyUpdatesTask> {
    val acceptedVersion = "(RELEASE|FINAL|GA|-ce|^[0-9,.v-]+(-r)?)$"
        .toRegex(RegexOption.IGNORE_CASE)
    rejectVersionIf {
        !acceptedVersion.containsMatchIn(candidate.version)
    }
}

fun Project.propertyOrEnv(propertyName: String, envName: String): String? {
    return if (hasProperty(propertyName)) {
        property(propertyName)?.toString()
    } else {
        System.getenv(envName)
    }
}

nexusPublishing {
    repositories {
        sonatype {
            username.set(propertyOrEnv("ossrh.user", "OSSRH_USER"))
            password.set(propertyOrEnv("ossrh.password", "OSSRH_PASSWORD"))
        }
    }
}

tasks.wrapper {
    gradleVersion = "7.5.1"
}
