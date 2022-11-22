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

    val ktorVersion: String by project

    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")

    // to implement producers and consumers
    val okhttpVersion: String by project
    api("com.squareup.okhttp3:okhttp:$okhttpVersion")
    val orgJsonVersion: String by project
    api("org.json:json:$orgJsonVersion")

    // The production code uses the SLF4J logging API at compile time
    val slf4jVersion: String by project
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    val jacksonVersion: String by project
    testImplementation(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
    testImplementation("com.fasterxml.jackson.core:jackson-databind")
    val radarSchemasVersion: String by project
    testImplementation("org.radarbase:radar-schemas-commons:$radarSchemasVersion")
    val junitVersion: String by project
    testImplementation("junit:junit:$junitVersion")
    val mockitoVersion: String by project
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("com.squareup.okhttp3:mockwebserver:$okhttpVersion")
    testRuntimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")
}
