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
    val avroVersion: String by project
    api("org.apache.avro:avro:$avroVersion")
    api(kotlin("reflect"))

    val ktorVersion: String by project

    api(platform("io.ktor:ktor-bom:$ktorVersion"))
    api("io.ktor:ktor-client-core")
    api("io.ktor:ktor-client-cio")
    api("io.ktor:ktor-client-auth")
    implementation("io.ktor:ktor-client-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")

    api("org.radarbase:managementportal-client:0.9.0-SNAPSHOT")

    val coroutinesVersion: String by project
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")

    // The production code uses the SLF4J logging API at compile time
    val slf4jVersion: String by project
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    val jacksonVersion: String by project
    testImplementation(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
    testImplementation("com.fasterxml.jackson.core:jackson-databind")
    val radarSchemasVersion: String by project
    testImplementation("org.radarbase:radar-schemas-commons:$radarSchemasVersion")
    val mockitoVersion: String by project
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    val mockitoKotlinVersion: String by project
    testImplementation("org.mockito.kotlin:mockito-kotlin:$mockitoKotlinVersion")
    val okhttpVersion: String by project
    testImplementation("com.squareup.okhttp3:mockwebserver:$okhttpVersion")
    testRuntimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")
}
