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

// In this section you declare the dependencies for your production and test code
dependencies {

    api(libs.avro) {
        implementation(libs.commons.compress)
    }
    api(kotlin("reflect"))
    api(platform(libs.ktor.bom))
    api("io.ktor:ktor-client-core")
    api("io.ktor:ktor-client-cio")
    api("io.ktor:ktor-client-auth")
    api(libs.coroutines.core)

    implementation(project(":radar-commons-kotlin"))
    implementation("io.ktor:ktor-client-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    testImplementation(platform(libs.jackson.bom))
    testImplementation("com.fasterxml.jackson.core:jackson-databind")

    testImplementation(libs.radar.schemas.commons)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.kotlin)
    testImplementation(libs.mockwebserver)
}

allOpen {
    annotation("org.radarbase.config.OpenConfig")
}
