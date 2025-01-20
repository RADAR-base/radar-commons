package org.radarbase.gradle.plugin

import com.github.jk1.license.LicenseReportPlugin
import io.sentry.android.gradle.extensions.SentryPluginExtension
import io.sentry.jvm.gradle.SentryJvmPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.ApplicationPlugin
import org.gradle.api.plugins.JavaApplication
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.bundling.Compression
import org.gradle.api.tasks.bundling.Tar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.getValue
import org.gradle.kotlin.dsl.hasPlugin
import org.gradle.kotlin.dsl.maven
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.repositories
import org.gradle.kotlin.dsl.withType
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.KtlintExtension
import org.jlleitschuh.gradle.ktlint.KtlintPlugin

fun Project.radarKotlin(configure: RadarKotlinExtension.() -> Unit) {
    configure(configure)
}

interface RadarKotlinExtension {
    val javaVersion: Property<Int>
    val kotlinVersion: Property<String>
    val kotlinApiVersion: Property<String>
    val junitVersion: Property<String>
    val log4j2Version: Property<String>
    val slf4jVersion: Property<String>
    val ktlintVersion: Property<String>
    val sentryEnabled: Property<Boolean>
    val sentryOrganization: Property<String>
    val sentryProject: Property<String>
    val sentrySourceContextToken: Property<String>
    val openTelemetryAgentVersion: Property<String>
    val openTelemetryAgentEnabled: Property<Boolean>
}

class RadarKotlinPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = with(project) {
        val extension = extensions.create<RadarKotlinExtension>("radarKotlin").apply {
            javaVersion.convention(Versions.java)
            kotlinVersion.convention(Versions.kotlin)
            kotlinApiVersion.convention("")
            junitVersion.convention(Versions.junit)
            ktlintVersion.convention(Versions.ktlint)
            slf4jVersion.convention(Versions.slf4j)
            sentryEnabled.convention(false)
            sentryOrganization.convention("radar-base")
            sentryProject.convention(project.name)
            sentrySourceContextToken.convention("")
            openTelemetryAgentVersion.convention(Versions.opentelemetry)
            openTelemetryAgentEnabled.convention(false)
        }

        apply(plugin = "kotlin")
        apply<KtlintPlugin>()

        // SentryJvmPlugin will be removed in afterEvaluate when sentryEnabled == false.
        apply<SentryJvmPlugin>()

        repositories {
            mavenCentral {
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
                jvmTarget.set(
                    extension.javaVersion.map { JvmTarget.fromTarget(it.toString()) }
                )
                apiVersion.set(
                    extension.kotlinApiVersion.zip(extension.kotlinVersion) { apiVersion, kotlinVersion ->
                        if (apiVersion.isNotEmpty()) {
                            KotlinVersion.fromVersion(apiVersion)
                        } else {
                            kotlinVersion.toKotlinVersion()
                        }
                    }
                )
                languageVersion.set(
                    extension.kotlinVersion.map { it.toKotlinVersion() }
                )
            }
        }

        extensions.configure<KtlintExtension> {
            version.set(extension.ktlintVersion)
        }

        dependencies {
            configurations["testImplementation"](extension.junitVersion.map { "org.junit.jupiter:junit-jupiter-api:$it" })
            configurations["testRuntimeOnly"](extension.junitVersion.map { "org.junit.jupiter:junit-jupiter-engine:$it" })
            configurations["testRuntimeOnly"]("org.junit.platform:junit-platform-launcher")
        }

        tasks.withType<Test> {
            testLogging {
                events("passed", "skipped", "failed")
                showStandardStreams = true
                exceptionFormat = TestExceptionFormat.FULL
            }
            useJUnitPlatform()
        }

        tasks.withType<Tar> {
            compression = Compression.GZIP
            archiveExtension.set("tar.gz")
        }

        tasks.register("downloadDependencies") {
            doFirst {
                configurations["compileClasspath"].files
                configurations["runtimeClasspath"].files
                println("Downloaded all dependencies")
            }
            outputs.upToDateWhen { false }
        }

        tasks.register<Copy>("copyDependencies") {
            from(configurations.named("runtimeClasspath").map { it.files })
            into(layout.buildDirectory.dir("third-party"))
//            // Rename the jar for sentry-opentelementry agent so the javaagent command is version-independent.
//            if (extension.openTelemetryAgentEnabled.get()) {
//                from(configurations.named("implementation").map { it.files })
//                rename { fileName ->
//                    if (fileName == "sentry-opentelemetry-agent-${extension.openTelemetryAgentVersion.get()}.jar") {
//                        "sentry-opentelemetry-agent.jar"
//                    } else {
//                        fileName
//                    }
//                }
//            }
            doLast {
                println("Copied third-party runtime dependencies")
            }
        }

        apply<LicenseReportPlugin>()

        tasks.register<Tar>("collectLicenses") {
            from(
                fileTree(layout.buildDirectory.dir("reports/dependency-license")),
                rootDir.resolve("LICENSE"),
            )
            compression = Compression.GZIP
            destinationDirectory.set(layout.buildDirectory.dir("reports"))
            archiveBaseName.set("${project.name}-dependency-license")
            dependsOn(tasks["generateLicenseReport"])
        }

        afterEvaluate {
            if (extension.slf4jVersion.isPresent) {
                dependencies {
                    val implementation by configurations
                    implementation("org.slf4j:slf4j-api:${extension.slf4jVersion.get()}")
                }
            }
            if (extension.sentryEnabled.get()) {
                val sentry = extensions.get("sentry") as SentryPluginExtension
                sentry.org.set(extension.sentryOrganization)
                sentry.projectName.set(extension.sentryProject)
                if (extension.sentrySourceContextToken.isPresent &&
                    extension.sentrySourceContextToken.get().isNotEmpty()
                ) {
                    // Passing the source context token will activate upload of our source code to Sentry.
                    sentry.includeSourceContext.set(true)
                    sentry.authToken.set(extension.sentrySourceContextToken)
                }
            } else {
                plugins.removeIf({ it is SentryJvmPlugin })
            }
            if (extension.log4j2Version.isPresent) {
                dependencies {
                    val log4j2Version = extension.log4j2Version.get()

                    if (plugins.hasPlugin("application")) {
                        val runtimeOnly by configurations
                        runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:$log4j2Version")
                        runtimeOnly("org.apache.logging.log4j:log4j-core:$log4j2Version")
                        runtimeOnly("org.apache.logging.log4j:log4j-jul:$log4j2Version")
                        if (extension.sentryEnabled.get()) {
                            val annotationProcessor by configurations
                            annotationProcessor("org.apache.logging.log4j:log4j-core:$log4j2Version")
                        }
                    } else {
                        val testRuntimeOnly by configurations
                        testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:$log4j2Version")
                        testRuntimeOnly("org.apache.logging.log4j:log4j-core:$log4j2Version")
                        testRuntimeOnly("org.apache.logging.log4j:log4j-jul:$log4j2Version")
                    }
                }

                tasks.withType<Test> {
                    if ("java.util.logging.manager" !in systemProperties) {
                        systemProperty(
                            "java.util.logging.manager",
                            "org.apache.logging.log4j.jul.LogManager"
                        )
                    }
                }

                if (extension.openTelemetryAgentEnabled.get() && extension.sentryEnabled.get()) {
                    dependencies {
                        configurations["implementation"](extension.openTelemetryAgentVersion.map {
                            "io.sentry:sentry-opentelemetry-agent:$it" })
                    }
                }

                if (plugins.hasPlugin(ApplicationPlugin::class)) {
                    extensions.configure<JavaApplication> {
                        if (applicationDefaultJvmArgs.none { "-Djava.util.logging.manager=" in it }) {
                            applicationDefaultJvmArgs += "-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"
                        }
                    }
                }
            }
        }

        configurations.named("implementation") {
            resolutionStrategy.cacheChangingModulesFor(0, "SECONDS")
        }
    }

    companion object {
        fun String.toKotlinVersion() = KotlinVersion.fromVersion(
            splitToSequence('.')
                .take(2)
                .joinToString(separator = "."),
        )
    }
}
