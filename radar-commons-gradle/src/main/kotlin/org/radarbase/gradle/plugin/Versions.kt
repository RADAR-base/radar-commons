package org.radarbase.gradle.plugin

/*
 Versions are defined in this object because they cannot be read from the gradle project.
 IMPORTANT!! The versions in the following files must be kept in sync:
 - gradle/libs.versions.toml
 - radar-commons-gradle/gradle/libs.versions.toml
 - radar-commons-gradle/src/main/kotlin/org/radarbase/gradle/plugin/Versions.kt
*/
@Suppress("ktlint:standard:property-naming")
object Versions {
    const val gradle = "8.7"
    const val java = 17
    const val kotlin = "1.9.21"
    const val ktlint = "0.50.0" // Ruleset version
    const val junit = "5.10.0"
    const val slf4j = "2.0.16"
}
