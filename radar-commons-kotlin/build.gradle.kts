plugins {
    kotlin("plugin.serialization")
}

description = "Library for Kotlin utility classes and functions"

dependencies {
    val slf4jVersion: String by project
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    val coroutinesVersion: String by project
    api(platform("org.jetbrains.kotlinx:kotlinx-coroutines-bom:$coroutinesVersion"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    val ktorVersion: String by project
    api(platform("io.ktor:ktor-bom:$ktorVersion"))
    api("io.ktor:ktor-client-auth")
    implementation("io.ktor:ktor-client-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    testImplementation("org.hamcrest:hamcrest:2.2")
}
