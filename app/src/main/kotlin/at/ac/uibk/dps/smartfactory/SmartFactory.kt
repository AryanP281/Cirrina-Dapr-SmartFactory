package at.ac.uibk.dps.smartfactory

import at.ac.uibk.dps.smartfactory.actors.arm.ArmActorImpl
import at.ac.uibk.dps.smartfactory.actors.assemblycontroller.AssemblyControllerActorImpl
import at.ac.uibk.dps.smartfactory.actors.belt.BeltActorImpl
import at.ac.uibk.dps.smartfactory.actors.jobcontroller.JobControllerActorImpl
import at.ac.uibk.dps.smartfactory.actors.messageprocessor.MessageProcessorImpl
import at.ac.uibk.dps.smartfactory.actors.monitor.MonitorActorImpl
import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import io.dapr.actors.runtime.ActorRuntime
import io.dapr.client.DaprClientBuilder
import java.io.File
import java.time.Duration
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication class SmartFactory

val logger = LoggerFactory.getLogger(SmartFactory::class.java)
val metrics = MetricRegistry()

fun main(args: Array<String>) {
  val role = System.getenv("ROLE")

  ActorRuntime.getInstance().config.setActorIdleTimeout(Duration.ofHours(24))

  when (role) {
    "jobcontroller" -> ActorRuntime.getInstance().registerActor(JobControllerActorImpl::class.java)
    "monitor" -> ActorRuntime.getInstance().registerActor(MonitorActorImpl::class.java)
    "messageprocessor" -> ActorRuntime.getInstance().registerActor(MessageProcessorImpl::class.java)
    "belt" -> ActorRuntime.getInstance().registerActor(BeltActorImpl::class.java)
    "arm" -> ActorRuntime.getInstance().registerActor(ArmActorImpl::class.java)
    "assemblycontroller" ->
      ActorRuntime.getInstance().registerActor(AssemblyControllerActorImpl::class.java)
    else -> logger.error("Unknown role $role")
  }

  // Creating and saving persistent variables
  val daprClient = DaprClientBuilder().build()
  daprClient.saveState("statestore", "isJobDone", false).block()
  daprClient.saveState("statestore", "logs", mutableListOf<String>()).block()
  daprClient.saveState("statestore", "productsCompleted", 0).block()

  // Initializing metrics
  val metricsPeriod = System.getenv("METRICS_PERIOD")?.toLong() ?: 1L
  CsvReporter.forRegistry(metrics)
    .build(File("./metrics/${System.getenv("ACTOR_ID") ?: "actor-0"}"))
    .start(metricsPeriod, TimeUnit.SECONDS)

  runApplication<SmartFactory>(*args)
}
