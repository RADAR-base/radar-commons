@Suppress("ConstPropertyName", "MemberVisibilityCanBePrivate")
object Versions {
    const val project = "1.2.0"

    object Plugins {
        const val licenseReport = "2.5"
        const val kotlin = "1.9.21"
        const val dokka = "1.9.10"
        const val kotlinSerialization = kotlin
        const val kotlinAllOpen = kotlin
        const val avro = "1.8.0"
        const val gradle = "8.3"
        const val publishPlugin = "2.0.0-rc-1"
    }

    const val java = 17
    const val slf4j = "2.0.13"
    const val confluent = "7.6.0"
    const val kafka = "${confluent}-ce"
    const val avro = "1.12.0"
    const val jackson = "2.15.3"
    const val okhttp = "4.12.0"
    const val junit = "5.10.0"
    const val mockito = "5.5.0"
    const val mockitoKotlin = "5.1.0"
    const val hamcrest = "2.2"
    const val radarSchemas = "0.8.8"
    const val opencsv = "5.8"
    const val ktor = "2.3.4"
    const val coroutines = "1.7.3"
    const val commonsCompress = "1.26.0"
    const val snappy = "1.1.10.5"
    const val guava = "32.1.1-jre"
    const val gradleVersionsPlugin = "0.50.0"
    const val ktlint = "12.0.3"
}
