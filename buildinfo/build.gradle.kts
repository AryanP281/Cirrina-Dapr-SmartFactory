plugins { kotlin("jvm") }

repositories { mavenCentral() }

val versionText =
  providers
    .fileContents(rootProject.layout.projectDirectory.file("version.txt"))
    .asText
    .get()
    .trim()
val generatedDir = layout.buildDirectory.dir("generated/buildinfo").get().asFile

val generateBuildInfo by tasks.registering {
  val outputFile = File(generatedDir, "at/ac/uibk/dps/projectname/generated/BuildInfo.kt")
  outputs.file(outputFile)

  doLast {
    val gitHash =
      System.getenv("GIT_HASH")
        ?: try {
          val proc = ProcessBuilder("git", "rev-parse", "--short", "HEAD").start()
          val output = proc.inputStream.bufferedReader().readText().trim()
          if (proc.waitFor() == 0 && output.isNotEmpty()) output else "unknown"
        } catch (e: Exception) {
          "unknown"
        }

    val timestamp = (System.currentTimeMillis() / 1000).toInt()

    outputFile.parentFile.mkdirs()
    outputFile.writeText(
      """
      package at.ac.uibk.dps.projectname.generated

      object BuildInfo {
          const val VERSION = "$versionText"
          const val REVISION = "$gitHash"
          const val TIMESTAMP = $timestamp
          const val INFO = "${'$'}VERSION - ${'$'}REVISION - ${'$'}TIMESTAMP"
      }
      """
        .trimIndent()
    )
  }
}

sourceSets["main"].kotlin.srcDir(generatedDir)

tasks.named("compileKotlin") { dependsOn(generateBuildInfo) }
