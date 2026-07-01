plugins {
  `java-library`
  id("com.ncorti.ktfmt.gradle")
  kotlin("kapt")
  kotlin("jvm")
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

  implementation(kotlin("stdlib-jdk8"))

  // Logging
  implementation("io.github.oshai:kotlin-logging-jvm:8.0.4")
  testImplementation("ch.qos.logback:logback-classic:1.5.34")

  // Dagger
  implementation("com.google.dagger:dagger:2.59.2")
  kapt("com.google.dagger:dagger-compiler:2.59.2")

  // Guava
  implementation("com.google.guava:guava:33.6.0-jre")

  // JUnit
  testImplementation(platform("org.junit:junit-bom:5.11.0"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation("org.junit-pioneer:junit-pioneer:2.3.0")
}

repositories {
  mavenCentral()
  gradlePluginPortal()
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
  compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_25) }
}

tasks.named("compileKotlin") { dependsOn(tasks.named("ktfmtFormat")) }

tasks.named<Test>("test") { useJUnitPlatform() }

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
