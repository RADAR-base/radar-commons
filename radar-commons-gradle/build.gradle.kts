import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `kotlin-dsl`
    `java-gradle-plugin`
    kotlin("jvm") version "1.9.0"
    `maven-publish`
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0-rc-1"
    id("org.jetbrains.dokka") version "1.9.0"
    signing
}

version = "1.1.1"
group = "org.radarbase"
description = "RADAR-base common Gradle plugin setup"

val githubUrl = "https://github.com/RADAR-base/radar-commons"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:1.9.10")
    implementation("org.jetbrains.dokka:dokka-gradle-plugin:1.9.0")
    implementation("com.github.ben-manes:gradle-versions-plugin:0.50.0")
    implementation("io.github.gradle-nexus:publish-plugin:2.0.0-rc-1")
    implementation("org.jlleitschuh.gradle:ktlint-gradle:11.6.0")
    implementation("com.github.jk1.dependency-license-report:com.github.jk1.dependency-license-report.gradle.plugin:2.5")
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
    options.release.set(11)
}
tasks.withType<KotlinCompile> {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_11)
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
                    id.set("blootsvoets")
                    name.set("Joris Borgdorff")
                    email.set("joris@thehyve.nl")
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
