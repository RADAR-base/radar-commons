import io.github.gradlenexus.publishplugin.NexusPublishExtension
import io.github.gradlenexus.publishplugin.NexusPublishPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.wrapper.Wrapper
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.repositories

class RootConventionsPlugin : Plugin<Project> {
    override fun apply(project: Project) = with(project) {
        allprojects {
            version = Versions.project
            group = "org.radarbase"
        }

        tasks.named<Wrapper>("wrapper") {
            gradleVersion = Versions.wrapper
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
