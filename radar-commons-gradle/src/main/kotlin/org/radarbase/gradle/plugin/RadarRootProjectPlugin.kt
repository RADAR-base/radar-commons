package org.radarbase.gradle.plugin

import io.github.gradlenexus.publishplugin.NexusPublishExtension
import io.github.gradlenexus.publishplugin.NexusPublishPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.provider.Property
import org.gradle.api.tasks.wrapper.Wrapper
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.named

fun Project.radarRootProject(configure: RadarRootProjectExtension.() -> Unit) {
    configure(configure)
}

interface RadarRootProjectExtension {
    val group: Property<String>
    val projectVersion: Property<String>
    val gradleVersion: Property<String>
}

class RadarRootProjectPlugin : Plugin<Project> {
    override fun apply(project: Project) = with(project) {
        val extension = extensions.create<RadarRootProjectExtension>("radarRootProject").apply {
            group.convention("org.radarbase")
            gradleVersion.convention(Versions.wrapper)
        }

        allprojects {
            afterEvaluate {
                version = extension.projectVersion.get()
                group = extension.group.get()
            }
        }

        afterEvaluate {
            tasks.named<Wrapper>("wrapper") {
                gradleVersion = extension.gradleVersion.get()
            }
        }

        apply<NexusPublishPlugin>()

        project.extensions.configure<NexusPublishExtension> {
            repositories {
                sonatype {
                    username.set(propertyOrEnv("ossrh.user", "OSSRH_USER"))
                    password.set(propertyOrEnv("ossrh.password", "OSSRH_PASSWORD"))
                }
            }
        }
    }

    companion object {
        private fun Project.propertyOrEnv(propertyName: String, envName: String): String? {
            return if (hasProperty(propertyName)) {
                property(propertyName)?.toString()
            } else {
                System.getenv(envName)
            }
        }
    }
}
