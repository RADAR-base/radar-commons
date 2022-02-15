# RADAR-Commons
[![Build Status](https://travis-ci.org/RADAR-base/radar-commons.svg?branch=master)](https://travis-ci.org/RADAR-base/radar-commons)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9fe7a419c83e4798af671e468c7e91cf)](https://www.codacy.com/app/RADAR-base/radar-commons?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-base/radar-commons&amp;utm_campaign=Badge_Grade)

Common utilities library containing basic schemas, streaming features, testing bridges and utils.

# Usage

Add the RADAR-Commons library to your project with Gradle by updating your `build.gradle` file with:

```gradle
repositories {
    mavenCentral()
}

dependencies {
    implementation group: 'org.radarbase', name: 'radar-commons', version: '0.14.0'
}
```

Example use, after adding [`radar-schemas`](https://github.com/radar-base/radar-schemas) to classpath:
```kotlin
// Set URLs for RADAR-base installation
val baseUrl = "..."
val kafkaUrl = "$baseUrl/kafka/"
val schemaUrl = "$baseUrl/schema/"
val oauthHeaders = ...
val key = ObservationKey("myProject", "myUser", "mySource")

// Configure RADAR-base clients
val client = RestClient.global().apply {
    server(ServerConfig(kafkaUrl))
    gzipCompression(true)
}.build()

val schemaRetriever = SchemaRetriever(ServerConfig(schemaUrl), 30)

val restSender = RestSender.Builder().apply {
    httpClient(client)
    schemaRetriever(schemaRetriever)
    useBinaryContent(true)
    headers(oauthHeaders)
}.build()

val sender = BatchedKafkaSender(restSender, 60_000L, 1000L)

// Configure topic to send data over
val topic = AvroTopic("linux_raspberry_temperature",
  ObservationKey.getClassSchema(), RaspberryTemperature.getClassSchema(),
  ObservationKey::class.java, RaspberryTemperature::class.java)

// Send data to topic. Be sure to close
// the sender after use. Preferably, a sender is reused
// for many observations so that requests are efficiently
// batched.
sender.sender(topic).use { topicSender ->
  readValuesFromSystem() { value ->
    topicSender.send(key, value)
  }
}
```
Note that this code above does not include any flows for registering a source with the managmentportal.

For server utilities, include `radar-commons-server`:
```gradle
repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
    implementation group: 'org.radarbase', name: 'radar-commons-server', version: '0.14.0'
}
```

For mocking clients of the RADAR-base infrastructure, use that 'radar-commons-testing' repository:

```gradle
repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
    testImplementation group: 'org.radarbase', name: 'radar-commons-testing', version: '0.14.0'
}
```

To test your backend with a MockProducer, copy `testing/mock.yml.template` to `testing/mock.yml` and edit its parameters. Then run
```
./gradlew :testing:run
```
to send data to your backend.

## Contributing

For latest code use `dev` branch. This is released on JFrog's OSS Artifactory. To use that release, add the following fragment to your `build.gradle` file.

```gradle
repositories {
    maven { url  'https://oss.sonatype.org/content/repositories/snapshots' }
}

configurations.all {
    // Check for updates every build
    resolutionStrategy.cacheChangingModulesFor 0, 'SECONDS'
}

dependencies {
    compile group: 'org.radarbase', name: 'radar-commons', version: '0.14.1-SNAPSHOT'
}
```

Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-base/radar-commons/issues), and please make a pull request.
