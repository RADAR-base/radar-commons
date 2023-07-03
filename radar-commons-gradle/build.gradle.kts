import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `kotlin-dsl`
    `java-gradle-plugin`
    kotlin("jvm") version "1.8.10"
    `maven-publish`
}

version = "1.0.1"
group = "org.radarbase"
description = "RADAR common Gradle plugins"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:1.8.22")
    implementation("org.jetbrains.dokka:dokka-gradle-plugin:1.8.20")
    implementation("com.github.ben-manes:gradle-versions-plugin:0.47.0")
    implementation("io.github.gradle-nexus:publish-plugin:1.3.0")
    implementation("org.jlleitschuh.gradle:ktlint-gradle:11.4.2")
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
    kotlinOptions {
        jvmTarget = "11"
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            setUrl("https://maven.pkg.github.com/radar-base/radar-commons")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                    ?: extra.properties["gpr.user"] as? String
                password = System.getenv("GITHUB_TOKEN")
                    ?: extra.properties["gpr.key"] as? String
            }
        }
    }
}
