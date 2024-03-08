plugins {
    kotlin("plugin.serialization")
}

description = "Library for Kotlin utility classes and functions"

dependencies {
    api(platform("org.jetbrains.kotlinx:kotlinx-coroutines-bom:${Versions.coroutines}"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${Versions.coroutines}")

    api(platform("io.ktor:ktor-bom:${Versions.ktor}"))
    api("io.ktor:ktor-client-auth:${Versions.ktor}")
    implementation("io.ktor:ktor-client-content-negotiation:${Versions.ktor}")
    implementation("io.ktor:ktor-serialization-kotlinx-json:${Versions.ktor}")

    testImplementation("org.hamcrest:hamcrest:${Versions.hamcrest}")
}
