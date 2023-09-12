# radar-commons-gradle

A Gradle plugin to do some common RADAR-base tasks.

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
