plugins {
  id("com.ncorti.ktfmt.gradle")
  kotlin("jvm")
  id("org.jetbrains.kotlinx.benchmark") version "0.4.17"
}

group = "ac.at.uibk.dps.projectname"

version =
  providers
    .fileContents(rootProject.layout.projectDirectory.file("version.txt"))
    .asText
    .get()
    .trim()

java { toolchain { languageVersion.set(JavaLanguageVersion.of(25)) } }

ktfmt { googleStyle() }

dependencies {
  implementation(project(":buildinfo"))
  implementation(project(":lib"))

  implementation(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.17")
}

repositories {
  mavenCentral()
  gradlePluginPortal()
}

benchmark { targets { register("main") } }

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
  compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_25) }
}

tasks.named("compileKotlin") { dependsOn(tasks.named("ktfmtFormat")) }

tasks.withType<Jar>().configureEach {
  manifest {
    attributes(
      mapOf(
        "Implementation-Version" to project.version.toString(),
        "Enable-Native-Access" to "ALL-UNNAMED",
      )
    )
  }
}
