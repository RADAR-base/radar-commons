description = "Library for Kotlin utility classes and functions"

dependencies {
    val slf4jVersion: String by project
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    val coroutinesVersion: String by project
    api(platform("org.jetbrains.kotlinx:kotlinx-coroutines-bom:$coroutinesVersion"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    testImplementation("org.hamcrest:hamcrest:2.2")
}
