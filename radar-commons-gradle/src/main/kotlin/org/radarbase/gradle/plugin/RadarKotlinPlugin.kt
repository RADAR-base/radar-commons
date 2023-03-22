package org.radarbase.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.provider.Property
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.kotlin.dsl.*
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.KtlintExtension
import org.jlleitschuh.gradle.ktlint.KtlintPlugin

fun Project.radarKotlin(configure: RadarKotlinExtension.() -> Unit) {
    configure<RadarKotlinExtension>(configure)
}

interface RadarKotlinExtension {
    val javaVersion: Property<Int>
    val kotlinVersion: Property<String>
    val junitVersion: Property<String>
    val ktlintVersion: Property<String>
}

class RadarKotlinPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = with(project) {
        val extension = extensions.create<RadarKotlinExtension>("radarKotlin").apply {
            javaVersion.convention(Versions.java)
            kotlinVersion.convention(Versions.kotlin)
            junitVersion.convention(Versions.junit)
            ktlintVersion.convention(Versions.ktlint)
        }

        apply(plugin = "kotlin")
        apply<KtlintPlugin>()

        repositories {
            mavenCentral() {
                mavenContent {
                    releasesOnly()
                }
            }
            mavenLocal()
            maven(url = "https://packages.confluent.io/maven/") {
                mavenContent {
                    releasesOnly()
                }
            }
            maven(url = "https://oss.sonatype.org/content/repositories/snapshots") {
                mavenContent {
                    snapshotsOnly()
                }
            }
        }

        tasks.withType<JavaCompile> {
            options.release.set(extension.javaVersion)
        }

        tasks.withType<KotlinCompile> {
            compilerOptions {
                jvmTarget.set(extension.javaVersion.map { JvmTarget.fromTarget(it.toString()) })
                val kotlinVersion = extension.kotlinVersion.map { version ->
                    KotlinVersion.fromVersion(
                        version
                            .splitToSequence('.')
                            .take(2)
                            .joinToString(separator = "."),
                    )
                }
                apiVersion.set(kotlinVersion)
                languageVersion.set(kotlinVersion)
            }
        }

        extensions.configure<KtlintExtension> {
            version.set(extension.ktlintVersion)
        }

        dependencies {
            configurations["testImplementation"](extension.junitVersion.map { "org.junit.jupiter:junit-jupiter-api:$it" })
            configurations["testRuntimeOnly"](extension.junitVersion.map { "org.junit.jupiter:junit-jupiter-engine:$it" })
        }

        configurations.named("implementation") {
            resolutionStrategy.cacheChangingModulesFor(0, "SECONDS")
        }
    }
}
