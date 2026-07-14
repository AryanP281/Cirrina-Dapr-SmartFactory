plugins {
    kotlin("jvm")
    `maven-publish`
}

group = "at.ac.uibk.dps.smartfactory"
version = rootProject.version

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))

    // Fory
    implementation("org.apache.fory:fory-core:0.15.0")
    implementation("org.apache.fory:fory-kotlin:0.15.0")
}