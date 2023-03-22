plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:1.8.10")
    implementation("org.jetbrains.dokka:dokka-gradle-plugin:1.8.10")
    implementation("com.github.ben-manes:gradle-versions-plugin:0.46.0")
    implementation("io.github.gradle-nexus:publish-plugin:1.3.0")
    implementation("org.jlleitschuh.gradle:ktlint-gradle:11.3.1")
}
