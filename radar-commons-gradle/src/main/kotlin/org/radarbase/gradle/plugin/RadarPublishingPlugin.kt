package org.radarbase.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.provider.Property
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPomDeveloperSpec
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.bundling.Compression
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.bundling.Tar
import org.gradle.kotlin.dsl.*
import org.gradle.plugins.signing.Sign
import org.gradle.plugins.signing.SigningExtension
import org.gradle.plugins.signing.SigningPlugin
import org.jetbrains.dokka.gradle.DokkaPlugin

fun Project.radarPublishing(configure: RadarPublishingExtension.() -> Unit) {
    configure(configure)
}

interface RadarPublishingExtension {
    val githubUrl: Property<String>
    val developers: Property<MavenPomDeveloperSpec.() -> Unit>

    fun developers(configure: MavenPomDeveloperSpec.() -> Unit) {
        developers.set(configure)
    }
}

class RadarPublishingPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = with(project) {
        val extension = extensions.create<RadarPublishingExtension>("radarPublishing")

        val sourcesJar by tasks.registering(Jar::class) {
            from(project.the<SourceSetContainer>()["main"].allSource)
            archiveClassifier.set("sources")
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
            val classes by tasks
            dependsOn(classes)
        }

        apply<DokkaPlugin>()

        val dokkaJar by tasks.registering(Jar::class) {
            from(layout.buildDirectory.dir("javadoc"))
            archiveClassifier.set("javadoc")
            val dokkaJavadoc by tasks
            dependsOn(dokkaJavadoc)
        }

        tasks.withType<Tar> {
            compression = Compression.GZIP
            archiveExtension.set("tar.gz")
        }

        tasks.withType<Jar> {
            manifest {
                attributes(
                    "Implementation-Title" to project.name,
                    "Implementation-Version" to project.version
                )
            }
        }

        apply<MavenPublishPlugin>()

        val assemble by tasks
        assemble.dependsOn(sourcesJar)
        assemble.dependsOn(dokkaJar)

        val publishingExtension = extensions.getByName<PublishingExtension>("publishing")
        val mavenJar by publishingExtension.publications.creating(MavenPublication::class) {
            from(components["java"])

            artifact(sourcesJar)
            artifact(dokkaJar)

            afterEvaluate {
                val githubUrl = requireNotNull(extension.githubUrl.orNull) { "Missing githubUrl value in radarPublishing" }
                pom {
                    name.set(project.name)
                    description.set(project.description)
                    url.set(githubUrl)
                    licenses {
                        license {
                            name.set("The Apache Software License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                            distribution.set("repo")
                        }
                    }
                    if (extension.developers.isPresent) {
                        developers {
                            val developerBlock = extension.developers.get()
                            developerBlock()
                        }
                    }
                    issueManagement {
                        system.set("GitHub")
                        url.set("$githubUrl/issues")
                    }
                    organization {
                        name.set("RADAR-base")
                        url.set("https://radar-base.org")
                    }
                    scm {
                        connection.set("scm:git:$githubUrl")
                        url.set(githubUrl)
                    }
                }
            }
        }

        apply<SigningPlugin>()

        extensions.configure<SigningExtension>("signing") {
            useGpgCmd()
            isRequired = true
            sign(tasks["sourcesJar"], tasks["dokkaJar"])
            sign(mavenJar)
        }

        tasks.withType<Sign> {
            onlyIf { gradle.taskGraph.hasTask(tasks["publish"]) }
        }
    }
}
