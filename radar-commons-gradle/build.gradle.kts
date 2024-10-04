import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `kotlin-dsl`
    `java-gradle-plugin`
    // Match to the versions in the bottom of this file
    kotlin("jvm") version "1.9.24"
    `maven-publish`
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    id("org.jetbrains.dokka") version "1.9.10"
    signing
}

version = Versions.project
group = "org.radarbase"
description = "RADAR-base common Gradle plugin setup"

val githubUrl = "https://github.com/RADAR-base/radar-commons"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:${Versions.Plugins.kotlin}")
    implementation("org.jetbrains.dokka:dokka-gradle-plugin:${Versions.Plugins.dokka}")
    implementation("com.github.ben-manes:gradle-versions-plugin:${Versions.gradleVersionsPlugin}")
    implementation("io.github.gradle-nexus:publish-plugin:${Versions.Plugins.publishPlugin}")
    implementation("org.jlleitschuh.gradle:ktlint-gradle:${Versions.ktlint}")
    implementation("com.github.jk1.dependency-license-report:com.github.jk1.dependency-license-report.gradle.plugin:${Versions.Plugins.licenseReport}")
    implementation("io.sentry.jvm.gradle:io.sentry.jvm.gradle.gradle.plugin:${Versions.sentry}")
}

gradlePlugin {
    plugins {
        create("radarRootProject") {
            id = "org.radarbase.radar-root-project"
            implementationClass = "org.radarbase.gradle.plugin.RadarRootProjectPlugin"
        }
        create("radarPublishing") {
            id = "org.radarbase.radar-publishing"
            implementationClass = "org.radarbase.gradle.plugin.RadarPublishingPlugin"
        }
        create("radarDependencyManagement") {
            id = "org.radarbase.radar-dependency-management"
            implementationClass = "org.radarbase.gradle.plugin.RadarDependencyManagementPlugin"
        }
        create("radarKotlin") {
            id = "org.radarbase.radar-kotlin"
            implementationClass = "org.radarbase.gradle.plugin.RadarKotlinPlugin"
        }
    }
}

tasks.withType<JavaCompile> {
    options.release.set(Versions.java)
}

tasks.withType<KotlinCompile> {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        languageVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9)
        apiVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_1_9)
    }
}

tasks.withType<Jar> {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version
        )
    }
}

val sourcesJar by tasks.registering(Jar::class) {
    from(sourceSets["main"].allSource)
    archiveClassifier.set("sources")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    val classes by tasks
    dependsOn(classes)
}

val dokkaJar by tasks.registering(Jar::class) {
    from(layout.buildDirectory.dir("dokka/javadoc"))
    archiveClassifier.set("javadoc")
    val dokkaJavadoc by tasks
    dependsOn(dokkaJavadoc)
}

tasks.withType<GenerateMavenPom> {
    afterEvaluate {
        pom.apply {
            name.set(project.name)
            url.set(githubUrl)
            description.set(project.description)
            licenses {
                license {
                    name.set("The Apache Software License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    distribution.set("repo")
                }
            }
            developers {
                developer {
                    id.set("bdegraaf1234")
                    name.set("Bastiaan de Graaf")
                    email.set("bastiaan@thehyve.nl")
                    organization.set("The Hyve")
                }
            }
            issueManagement {
                system.set("GitHub")
                url.set("$githubUrl/issues")
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

publishing {
    publications {
        withType<MavenPublication> {
            artifact(sourcesJar)
            artifact(dokkaJar)
        }
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
    this.repositories {
        sonatype {
            username.set(propertyOrEnv("ossrh.user", "OSSRH_USER"))
            password.set(propertyOrEnv("ossrh.password", "OSSRH_PASSWORD"))
        }
    }
}

signing {
    useGpgCmd()
    isRequired = true
    afterEvaluate {
        publishing.publications.forEach { sign(it) }
    }
    sign(tasks["sourcesJar"])
    sign(tasks["dokkaJar"])
}

tasks.withType<Sign> {
    onlyIf { gradle.taskGraph.hasTask(project.tasks["publish"]) }
    dependsOn(sourcesJar)
    dependsOn(dokkaJar)
}

tasks.withType<PublishToMavenRepository> {
    dependsOn(tasks.withType<Sign>())
}

// Because this project is where all the required plugins get built, we need to add the dependencies separately here.
// They should be copied from the Versions.kt file directly to maintain consistency.
@Suppress("ConstPropertyName", "MemberVisibilityCanBePrivate")
object Versions {
    const val project = "1.2.0"

    object Plugins {
        const val licenseReport = "2.5"
        const val kotlin = "1.9.21"
        const val dokka = "1.9.10"
        const val kotlinSerialization = kotlin
        const val kotlinAllOpen = kotlin
        const val avro = "1.8.0"
        const val gradle = "8.3"
        const val publishPlugin = "2.0.0-rc-1"
    }

    const val java = 17
    const val slf4j = "2.0.13"
    const val confluent = "7.6.0"
    const val kafka = "${confluent}-ce"
    const val avro = "1.12.0"
    const val jackson = "2.15.3"
    const val okhttp = "4.12.0"
    const val junit = "5.10.0"
    const val mockito = "5.5.0"
    const val mockitoKotlin = "5.1.0"
    const val hamcrest = "2.2"
    const val radarSchemas = "0.8.8"
    const val opencsv = "5.8"
    const val ktor = "2.3.4"
    const val coroutines = "1.7.3"
    const val commonsCompress = "1.26.0"
    const val snappy = "1.1.10.5"
    const val guava = "32.1.1-jre"
    const val gradleVersionsPlugin = "0.50.0"
    const val ktlint = "12.0.3"
    const val sentry = "4.10.0"
}
