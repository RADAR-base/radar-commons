plugins {
    alias(libs.plugins.kotlin.serialization)
}

description = "Library for Kotlin utility classes and functions"

dependencies {
    // Kotlin Coroutines
    api(platform(libs.kotlinx.coroutines.bom))
    api(libs.kotlinx.coroutines.core)

    // Ktor Client
    api(platform(libs.ktor.bom))
    api(libs.ktor.client.auth)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)

    // Testing
    testImplementation(libs.hamcrest)
    testImplementation(libs.kotlinx.coroutines.test)
    testRuntimeOnly(libs.slf4j.simple)
}

