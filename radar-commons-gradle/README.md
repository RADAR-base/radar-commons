# radar-commons-gradle

A Gradle plugin to do some common RADAR-base tasks.

## Usage

Add the following block to `settings.gradle.kts` to get access to the RADAR-base plugins.

```gradle
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven(url = "https://maven.pkg.github.com/radar-base/radar-commons") {
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                    ?: extra.properties["gpr.user"] as? String
                    ?: extra.properties["public.gpr.user"] as? String
                password = System.getenv("GITHUB_TOKEN")
                    ?: extra.properties["gpr.token"] as? String
                    ?: (extra.properties["public.gpr.token"] as? String)?.let {
                        Base64.getDecoder().decode(it).decodeToString()
                    }
            }
        }
    }
}
```

We recommend to store a Base64 encoded PAT in your projects' `gradle.properties` with only `read:packages` access, created in your [GitHub Developer settings](https://github.com/settings/tokens/new?scopes=read:packages&description=GPR%20for%20Gradle). The Base64 encoded token should be stored as `public.gpr.token` and the associated username as `public.gpr.user`. To use your personal PAT, store the PAT in `~/.gradle/gradle.properties` with keys `gpr.user` and `gpr.token`. Use the following PAT if needed
```properties
public.gpr.user=radar-public
public.gpr.token=Z2hwX0h0d0FHSmJzeEpjenBlUVIycVhWb0RpNGdZdHZnZzJTMFVJZA==
```
Note that the above credentials may be changed or revoked at any time.

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
