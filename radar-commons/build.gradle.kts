plugins {
    kotlin("plugin.serialization")
}

description = "RADAR Common utilities library."

//---------------------------------------------------------------------------//
// Sources and classpath configurations                                      //
//---------------------------------------------------------------------------//

// In this section you declare where to find the dependencies of your project
repositories {
    maven(url = "https://jitpack.io")
}

// In this section you declare the dependencies for your production and test code
dependencies {
    api("org.apache.avro:avro:${Versions.avro}")
    api(kotlin("reflect"))

    implementation(project(":radar-commons-kotlin"))

    api(platform("io.ktor:ktor-bom:${Versions.ktor}"))
    api("io.ktor:ktor-client-core")
    api("io.ktor:ktor-client-cio")
    api("io.ktor:ktor-client-auth")
    implementation("io.ktor:ktor-client-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${Versions.coroutines}")

    // The production code uses the SLF4J logging API at compile time
    implementation("org.slf4j:slf4j-api:${Versions.slf4j}")

    testImplementation(platform("com.fasterxml.jackson:jackson-bom:${Versions.jackson}"))
    testImplementation("com.fasterxml.jackson.core:jackson-databind")
    testImplementation("org.radarbase:radar-schemas-commons:${Versions.radarSchemas}")
    testImplementation("org.mockito:mockito-core:${Versions.mockito}")
    testImplementation("org.mockito.kotlin:mockito-kotlin:${Versions.mockitoKotlin}")
    testImplementation("com.squareup.okhttp3:mockwebserver:${Versions.okhttp}")
    testRuntimeOnly("org.slf4j:slf4j-simple:${Versions.slf4j}")
}
