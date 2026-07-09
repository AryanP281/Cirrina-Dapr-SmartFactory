plugins {
  application
  id("org.springframework.boot") version "3.5.0"
  id("io.spring.dependency-management") version "1.1.7"
  id("com.ncorti.ktfmt.gradle")
  kotlin("kapt")
  kotlin("jvm")
  kotlin("plugin.spring") version "2.1.0"
}

group = "ac.at.uibk.dps.projectname"

version =
  providers
    .fileContents(rootProject.layout.projectDirectory.file("version.txt"))
    .asText
    .get()
    .trim()

application {
  mainClass.set("at.ac.uibk.dps.projectname.ProjectNameKt")
  applicationName = "projectname"
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(25)) } }

ktfmt { googleStyle() }

dependencies {
  implementation(project(":buildinfo"))
  implementation(project(":lib"))

  implementation(kotlin("stdlib-jdk8"))

  // Fory
  implementation("org.apache.fory:fory-core:0.15.0")
  implementation("org.apache.fory:fory-kotlin:0.15.0")

  // Dapr
  implementation("io.dapr:dapr-sdk:1.18.0")
  implementation("io.dapr:dapr-sdk-actors:1.18.0")
  implementation("io.dapr:dapr-sdk-springboot:1.18.0")

  // Spring Web
  implementation("org.springframework.boot:spring-boot-starter-web")

  // Logging
  implementation("io.github.oshai:kotlin-logging-jvm:8.0.4")
  implementation("ch.qos.logback:logback-classic:1.5.34")

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

tasks.named<Zip>("distZip") { archiveFileName.set("projectname.zip") }

tasks.withType<Jar>().configureEach {
  manifest {
    attributes(
      mapOf(
        "Main-Class" to "at.ac.uibk.dps.projectname.ProjectNameKt",
        "Implementation-Version" to project.version.toString(),
        "Enable-Native-Access" to "ALL-UNNAMED",
      )
    )
  }
}
