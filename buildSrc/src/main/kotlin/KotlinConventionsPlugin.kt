import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.kotlin.dsl.*
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

class KotlinConventionsPlugin : Plugin<Project> {
    override fun apply(project: Project) = with(project) {
        apply(plugin = "kotlin")

        repositories {
            mavenCentral() {
                mavenContent {
                    releasesOnly()
                }
            }
            mavenLocal()
            maven(url = "https://packages.confluent.io/maven/") {
                mavenContent {
                    releasesOnly()
                }
            }
            maven(url = "https://oss.sonatype.org/content/repositories/snapshots") {
                mavenContent {
                    snapshotsOnly()
                }
            }
        }

        dependencies {
            configurations["testImplementation"]("org.junit.jupiter:junit-jupiter-api:${Versions.junit}")
            configurations["testRuntimeOnly"]("org.junit.jupiter:junit-jupiter-engine:${Versions.junit}")
        }

        tasks.withType<JavaCompile> {
            options.release.set(Versions.java.toInt())
        }

        tasks.withType<KotlinCompile> {
            compilerOptions {
                jvmTarget.set(JvmTarget.fromTarget(Versions.java))
                val kotlinVersion = KotlinVersion.fromVersion(
                    Versions.Plugins.kotlinSerialization
                        .splitToSequence('.')
                        .take(2)
                        .joinToString(separator = "."),
                )
                apiVersion.set(kotlinVersion)
                languageVersion.set(kotlinVersion)
            }
        }

        afterEvaluate {
            configurations.named("implementation") {
                resolutionStrategy.cacheChangingModulesFor(0, "SECONDS")
            }
        }
    }
}
