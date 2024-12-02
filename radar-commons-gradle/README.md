# radar-commons-gradle

A Gradle plugin to do some common RADAR-base tasks.

<!-- TOC -->
* [radar-commons-gradle](#radar-commons-gradle)
  * [Usage](#usage)
  * [radar-commons-gradle plugin](#radar-commons-gradle-plugin)
    * [radarKotlin extension](#radarkotlin-extension)
      * [Enable logging with log4j2](#enable-logging-with-log4j2)
      * [Enable monitoring with Sentry](#enable-monitoring-with-sentry)
      * [Enable Sentry source context](#enable-sentry-source-context)
      * [Customizing Sentry configuration](#customizing-sentry-configuration)
<!-- TOC -->

## Usage

Add the following block to `settings.gradle.kts` to get access to the RADAR-base plugins.

```gradle
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}
```

Then use the plugins with the following root project configurations:

```gradle
import org.radarbase.gradle.plugin.radarKotlin
import org.radarbase.gradle.plugin.radarPublishing

plugins {
    val radarCommonsVersion = "..."
    id("org.radarbase.radar-root-project") version radarCommonsVersion 
    id("org.radarbase.radar-dependency-management") version radarCommonsVersion
    id("org.radarbase.radar-kotlin") version radarCommonsVersion apply false
    id("org.radarbase.radar-publishing") version radarCommonsVersion apply false
}

radarRootProject {
    projectVersion.set(Versions.project)
    group.set("org.radarbase") // is already default value
    gradleVersion.set(Versions.gradle) // already has a default value
}

radarDependencies {
    regex.set("(^[0-9,.v-]+(-r)?|RELEASE|FINAL|GA|-CE)$")  // default value
    // default value, if set to true then disregard major version
    // updates, e.g. 5.0.0 -> 6.0.0 is not allowed but 1.6.0 -> 1.7.0 is allowed.
    rejectMajorVersionUpdates.set(false)
}

subprojects {
    apply(plugin = "org.radarbase.radar-kotlin")
    apply(plugin = "org.radarbase.radar-publishing")

    radarKotlin {
        javaVersion.set(Versions.java) // already has a default value
        kotlinVersion.set(Versions.Plugins.kotlin) // already has a default value
        junitVersion.set(Versions.junit) // already has a default value
        ktlintVersion.set(Versions.ktlint) // already has a default value
        slf4jVersion.set(Versions.slf4j) // already has a default value
        // log4j2Version.set(Versions.log4j2) // setting this will enable log4j2
        // sentryVersion.set(Versions.sentry) // setting this will enable Sentry monitoring
        // sentryEnabled.set(false) // setting this to true will enable Sentry monitoring
        // sentrySourceContextToken.set("") // setting this will upload the source code context to Sentry
        // sentryOrganization.set("radar-base") // already has a default value, only needed when setting 'sentrySourceContextToken'
        // sentryProject.set("") // already has a default value, only needed when setting 'sentrySourceContextToken'
    }

    // Both values are required to be set to use radar-publishing.
    // This will force the use of GPG signing maven publications.
    radarPublishing {
        githubUrl.set("https://github.com/RADAR-base/my-project")
        developers {
            developer {
                id.set("myhandle")
                name.set("My Name")
                email.set("my@email.com")
                organization.set("My company")
            }
        }
    }
}
```

## radar-commons-gradle plugin

This plugin provides the basics for RADAR-base projects.

### radarKotlin extension

#### Enable logging with log4j2

By default, no logging implementation is added to projects (only the slf4j interface is included). To enable log4j2, add
the following to your root project configurations:

```gradle
...
subprojects {
    ...
    radarKotlin {
        log4j2Version.set(Versions.log4j2)
    }
    ...
}
```

#### Enable monitoring with Sentry

1. Activate log4j2 logging (see [above](#enable-logging-with-log4j2))
2. Activate Sentry in the _radarKotlin_ extension:

```gradle
...
subprojects {
    ...
    radarKotlin {
        sentryEnabled.set(true)
    }
    ...
}
```

3. Add a Sentry log Appender to the `log4j2.xml` configuration file to `src/main/resources`. For example:

```xml

<configuration status="warn">
    <appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout
                pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"
            />
        </Console>
        <Sentry name="Sentry" debug="false"/>
    </appenders>

    <loggers>
        <root level="INFO">
            <appender-ref ref="Console"/>
            <appender-ref ref="Sentry" level="WARN"/>
        </root>
    </loggers>
</configuration>
```

4. Set the `SENTRY_DSN` environment variable at runtime to the DSN of your Sentry project.

#### Enable Sentry source context

Sentry can be configured to show the source code context of the error. For this to work, source code can be uploaded
to the target sentry organization and project during the build phase. To enable this, set the following values:

```gradle
...
subprojects {
    ...
    radarKotlin {
        sentrySourceContextToken.set("my-token")
        sentryOrganization.set("my-organization")
        sentryProject.set("my-project")
    }
    ...
}
```

NOTE: The organization and project must correspond to the values for the monitoring environment in Sentry. The
organization and project are encooded in Sentry OAuth token.

#### Customizing Sentry configuration

In a project that uses radar-commons-gradle, the Sentry configuration can be customized by using the _sentry_ extension
like for instance:

build.gradle.kts

```gradle
sentry {
    debug = true
    ...
}
... 

Additional information on config options: https://docs.sentry.io/platforms/java/guides/log4j2/gradle/
