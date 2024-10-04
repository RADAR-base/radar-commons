# RADAR-Commons

Common utilities library containing basic schemas, streaming features, testing bridges and utils.

# Usage

Add the RADAR-Commons library to your project with Gradle by updating your `build.gradle.kts` file with:

```gradle
repositories {
    mavenCentral()
}

dependencies {
    implementation("org.radarbase:radar-commons:1.1.2")
}
```

Example use, after adding [`radar-schemas`](https://github.com/radar-base/radar-schemas) to classpath:

```kotlin
// Set URLs for RADAR-base installation
val baseUrl = "https://..."
val oauthToken = ...

val kafkaSender = restKafkaSender {
    baseUrl = "$baseUrl/kafka/"
    headers.append("Authorization", "Bearer $oauthToken")
    httpClient {
        timeout(10.seconds)
    }
    schemaRetriever ("$baseUrl/schema/")
}

// Configure topic to send data over
val topic = AvroTopic(
    "linux_raspberry_temperature",
    ObservationKey.getClassSchema(),
    RaspberryTemperature.getClassSchema(),
    ObservationKey::class.java,
    RaspberryTemperature::class.java
)

val topicSender = kafkaSender.sender(topic)

val key = ObservationKey("myProject", "myUser", "mySource")

// Send data to topic.
runBlocking {
    val values: List<RaspberryTemperature> = readValuesFromSystem()
    topicSender.send(key, values)
}
```

Note that this code above does not include any flows for registering a source with the ManagementPortal.

For server utilities, include `radar-commons-server`:
```gradle
repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    implementation("org.radarbase:radar-commons-server:1.1.2")
}
```

For mocking clients of the RADAR-base infrastructure, use that 'radar-commons-testing' repository:

```gradle
repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    testImplementation("org.radarbase:radar-commons-testing:1.1.2")
}
```

To test your backend with a MockProducer, copy `testing/mock.yml.template` to `testing/mock.yml` and edit its parameters. Then run
```
./gradlew :testing:run
```
to send data to your backend.

To use the RADAR Gradle plugins, see the README of the `radar-commons-gradle` directory.

## Contributing

For latest code use `dev` branch. This is released on JFrog's OSS Artifactory. To use that release, add the following fragment to your `build.gradle` file.

```gradle
repositories {
    maven(url = "https://oss.sonatype.org/content/repositories/snapshots")
}

configurations.all {
    // Check for updates every build
    resolutionStrategy.cacheChangingModulesFor(0, "SECONDS")
}

dependencies {
    implementation("org.radarbase:radar-commons:1.2.0")
}
```

Code should be formatted using the Kotlin official style guide, in addition to ktlint rules.
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-base/radar-commons/issues), and please make a pull request.
