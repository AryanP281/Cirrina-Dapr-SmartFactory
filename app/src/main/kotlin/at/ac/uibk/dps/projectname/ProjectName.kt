package at.ac.uibk.dps.projectname

import at.ac.uibk.dps.projectname.di.DaggerAppComponent
import at.ac.uibk.dps.projectname.generated.BuildInfo
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

fun main() {
  logger.info { "Version ${BuildInfo.VERSION}" }

  val component = DaggerAppComponent.create()

  logger.info { Primes.approximate().take(50).toList() }
}
