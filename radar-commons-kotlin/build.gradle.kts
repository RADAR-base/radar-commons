plugins {
    kotlin("plugin.serialization")
}

description = "Library for Kotlin utility classes and functions"

dependencies {
    api(platform("org.jetbrains.kotlinx:kotlinx-coroutines-bom:${Versions.coroutines}"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    api(platform("io.ktor:ktor-bom:${Versions.ktor}"))
    api("io.ktor:ktor-client-auth")
    implementation("io.ktor:ktor-client-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    testImplementation("org.hamcrest:hamcrest:2.2")
}
