package org.radarbase.gradle.plugin

// Versions are defined in this object because they cannot be read from the gradle project.
// IMPORTANT!! The versions in this file must be kept in sync with the versions.
@Suppress("ktlint:standard:property-naming")
object Versions {
    const val gradle = "8.7"
    const val java = 17
    const val kotlin = "1.9.21"
    const val ktlint = "0.50.0" // Ruleset version
    const val junit = "5.10.0"
    const val slf4j = "2.0.16"
}
