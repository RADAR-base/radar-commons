plugins {
    alias(libs.plugins.kotlin.serialization)
}

description = "Library for Kotlin utility classes and functions"

dependencies {
    api(platform(libs.coroutines.bom))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    api(platform(libs.ktor.bom))
    api("io.ktor:ktor-client-auth")
    implementation("io.ktor:ktor-client-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    testImplementation(libs.hamcrest)
}
