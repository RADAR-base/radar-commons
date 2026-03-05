import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `kotlin-dsl`
    `java-gradle-plugin`
    `maven-publish`
    signing
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.nexus.publish)
    alias(libs.plugins.dokka)
    alias(libs.plugins.dokka.javadoc)
}

version = libs.versions.project
group = "org.radarbase"
description = "RADAR-base common Gradle plugin setup"

val githubUrl = "https://github.com/RADAR-base/radar-commons"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

configurations.all {
    resolutionStrategy {
        /* The entries in the block below are added here to force the version of
        *  transitive dependencies and mitigate reported vulnerabilities */
        force(
            "com.fasterxml.jackson.core:jackson-databind:2.17.2"
        )
    }
}

dependencies {
    implementation(libs.gradlePlugin.kotlin)
    implementation(libs.gradlePlugin.dokka)
    implementation(libs.gradlePlugin.dokka.javadoc)
    implementation(libs.gradlePlugin.versions)
    implementation(libs.gradlePlugin.publish)
    implementation(libs.gradlePlugin.versionCatalogUpdate)
    implementation(libs.gradlePlugin.licenseReport)
    implementation(libs.gradlePlugin.sentry)
    implementation(libs.gradlePlugin.ktlint)
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
    options.release.set(libs.versions.java.get().toInt())
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
            "Implementation-Version" to project.version,
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
    dependsOn(tasks.dokkaGenerateJavadoc)
    from(tasks.dokkaGeneratePublicationJavadoc.flatMap { it.outputDirectory })
    archiveClassifier.set("javadoc")
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

fun Project.propertyOrEnv(
    propertyName: String,
    envName: String,
): String? =
    if (hasProperty(propertyName)) {
        property(propertyName)?.toString()
    } else {
        System.getenv(envName)
    }

nexusPublishing {
    this.repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
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
