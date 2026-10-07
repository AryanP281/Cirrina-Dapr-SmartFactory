import sun.jvmstat.monitor.MonitoredVmUtil.mainClass

plugins {
    application
    id("org.springframework.boot") version "3.5.0"
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "2.3.10"
    id("application")
    kotlin("plugin.spring") version "2.3.10"
}

group = "at.ac.uibk.dps.cirrina.execution.object"
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
    mainClass.set("at.ac.uibk.dps.cirrina.execution.object.MetricsCollectorKt")
}

tasks.test {
    useJUnitPlatform()
}

springBoot {
    mainClass.set("at.ac.uibk.dps.cirrina.execution.object.MetricsCollectorKt")
}

tasks.bootJar { archiveFileName.set("DaprMetricsCollector.jar") }