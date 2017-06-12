# RADAR-Commons
[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-Commons.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Commons)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9fe7a419c83e4798af671e468c7e91cf)](https://www.codacy.com/app/RADAR-CNS/RADAR-Commons?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-Commons&amp;utm_campaign=Badge_Grade)

Common utilities library containing basic schemas, streaming features, testing bridges and utils.

# Usage

Add the RADAR-Commons library to your project with Gradle by updating your `build.gradle` file with:

```gradle
repositories {
    jcenter()
}

dependencies {
    compile group: 'org.radarcns', name: 'radar-commons', version: '0.4'
}
```

For mocking clients of the RADAR-CNS infrastructure, use that 'radar-commons-testing' repository:

```gradle
repositories {
    maven { url  'http://dl.bintray.com/radar-cns/org.radarcns' }
}

dependencies {
    testCompile group: 'org.radarcns', name: 'radar-commons-testing', version: '0.4'
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
    maven { url  'http://oss.jfrog.org/artifactory/oss-snapshot-local/' }
}

dependencies {
    compile group: 'org.radarcns', name: 'radar-commons', version: '0.4.1-SNAPSHOT'
}
```

Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-CNS/RADAR-Commons/issues), and please make a pull request.
