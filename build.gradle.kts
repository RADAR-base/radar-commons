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
    alias(libs.plugins.avro) apply false
    id("org.radarbase.radar-root-project")
    id("org.radarbase.radar-dependency-management")
}

val githubRepoName = "RADAR-base/radar-commons"
val githubUrl = "https://github.com/$githubRepoName"

radarRootProject {
    projectVersion.set(properties["projectVersion"] as String)
    gradleVersion.set(libs.versions.gradle)
}

subprojects {
    // Apply the plugins
    apply(plugin = "org.radarbase.radar-kotlin")
    apply(plugin = "org.radarbase.radar-publishing")

    dependencies {
        configurations["testImplementation"](rootProject.libs.coroutines.test)
        configurations["testRuntimeOnly"](rootProject.libs.slf4j.simple)
    }

    radarPublishing {
        githubUrl.set("https://github.com/$githubRepoName")
        developers {
            developer {
                id.set("bdegraaf1234")
                name.set("Bastiaan de Graaf")
                email.set("bastiaan@thehyve.nl")
                organization.set("The Hyve")
            }
            developer {
                id.set("nivemaham")
                name.set("Nivethika Mahasivam")
                email.set("nivethika@thehyve.nl")
                organization.set("The Hyve")
            }
        }
    }

}
