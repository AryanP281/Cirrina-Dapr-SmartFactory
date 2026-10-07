import sun.jvmstat.monitor.MonitoredVmUtil.mainClass

plugins {
    application
    id("org.springframework.boot") version "3.5.0"
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "2.3.10"
    id("application")
    id("com.gradleup.shadow") version "9.0.0"
    kotlin("plugin.spring") version "2.3.10"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("io.dapr:dapr-sdk:1.18.0")
    implementation("io.dapr:dapr-sdk-springboot:1.18.0")

    implementation("org.springframework.boot:spring-boot-starter-web")

    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.16")
    implementation(kotlin("stdlib"))
}

kotlin {
    jvmToolchain(25)
}

application {
    mainClass.set("org.example.MainKt")
}

tasks.test {
    useJUnitPlatform()
}

springBoot {
    mainClass.set("org.example.MainKt")
}

tasks.bootJar { archiveFileName.set("DaprMetricsCollector.jar") }

tasks.shadowJar {
    archiveFileName.set("DaprMetricsCollector.jar")

    manifest {
        attributes["Main-Class"] = application.mainClass.get()
    }

    mergeServiceFiles()
}