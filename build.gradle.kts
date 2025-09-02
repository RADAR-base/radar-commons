import org.radarbase.gradle.plugin.radarKotlin
import org.radarbase.gradle.plugin.radarPublishing

/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
plugins {
    kotlin("plugin.serialization") version Versions.Plugins.kotlinSerialization apply false
    kotlin("plugin.allopen") version Versions.Plugins.kotlinAllOpen apply false
    id("com.github.davidmc24.gradle.plugin.avro") version Versions.Plugins.avro apply false
    id("org.radarbase.radar-root-project")
    id("org.radarbase.radar-dependency-management")
    id("org.radarbase.radar-kotlin") apply false
    id("org.radarbase.radar-publishing") apply false
}

val githubRepoName = "RADAR-base/radar-commons"
val githubUrl = "https://github.com/$githubRepoName"

radarRootProject {
    projectVersion.set(Versions.project)
    gradleVersion.set(Versions.Plugins.gradle)
}

subprojects {
    // Apply the plugins
    apply(plugin = "org.radarbase.radar-kotlin")
    apply(plugin = "org.radarbase.radar-publishing")

    configurations.all {
        resolutionStrategy {
            /* The entries in the block below are added here to force the version of
            *  transitive dependencies and mitigate reported vulnerabilities */
            force(
                "com.fasterxml.jackson.core:jackson-databind:2.17.2"
            )
        }
    }

    dependencies {
        configurations["testImplementation"]("org.jetbrains.kotlinx:kotlinx-coroutines-test:${Versions.coroutines}")
        configurations["testRuntimeOnly"]("org.slf4j:slf4j-simple:${Versions.slf4j}")
    }

    radarPublishing {
        githubUrl.set("https://github.com/$githubRepoName")
        developers {
            developer {
                id.set("pvannierop")
                email.set("pim@thehyve.nl")
                name.set("Pim van Nierop")
                organization.set("The Hyve")
            }
        }
    }

    radarKotlin {
        javaVersion.set(Versions.java)
        kotlinVersion.set(Versions.Plugins.kotlin)
        junitVersion.set(Versions.junit)
        slf4jVersion.set(Versions.slf4j)
    }
}
