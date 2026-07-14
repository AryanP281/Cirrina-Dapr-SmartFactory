package at.ac.uibk.dps.smartfactory

import at.ac.uibk.dps.smartfactory.actors.arm.ArmActorImpl
import at.ac.uibk.dps.smartfactory.actors.assemblycontroller.AssemblyControllerActorImpl
import at.ac.uibk.dps.smartfactory.actors.belt.BeltActorImpl
import at.ac.uibk.dps.smartfactory.actors.jobcontroller.JobControllerActorImpl
import at.ac.uibk.dps.smartfactory.actors.messageprocessor.MessageProcessorImpl
import at.ac.uibk.dps.smartfactory.actors.monitor.MonitorActorImpl
import io.dapr.actors.runtime.ActorRuntime
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication class SmartFactory

val logger = LoggerFactory.getLogger(SmartFactory::class.java)

fun main(args: Array<String>) {
  val role = System.getenv("ROLE")
  val actorId = System.getenv("ACTOR_ID") ?: "actor-0"

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

  runApplication<SmartFactory>(*args)
}
