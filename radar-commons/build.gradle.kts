plugins {
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.kotlin.allopen)
}

description = "RADAR Common utilities library."

// ---------------------------------------------------------------------------//
// Sources and classpath configurations                                      //
// ---------------------------------------------------------------------------//

// In this section you declare where to find the dependencies of your project
repositories {
    maven(url = "https://jitpack.io")
}

dependencies {
    api(libs.apache.avro) {
        implementation(libs.apache.commons.compress)
    }
    api(libs.kotlin.reflect)

    implementation(project(":radar-commons-kotlin"))

    api(platform(libs.ktor.bom))
    api(libs.ktor.client.core)
    api(libs.ktor.client.cio)
    api(libs.ktor.client.auth)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)

    api(libs.kotlinx.coroutines.core)

    testImplementation(platform(libs.jackson.bom))
    testImplementation(libs.jackson.databind)
    testImplementation(libs.radar.schemas.commons)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.kotlin)
    testImplementation(libs.okhttp.mockwebserver)
    testImplementation(libs.kotlinx.coroutines.test)
    testRuntimeOnly(libs.slf4j.simple)
}

allOpen {
    annotation("org.radarbase.config.OpenConfig")
}
