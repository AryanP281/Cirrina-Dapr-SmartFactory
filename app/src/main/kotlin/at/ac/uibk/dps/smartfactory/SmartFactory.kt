package at.ac.uibk.dps.smartfactory

import at.ac.uibk.dps.smartfactory.actors.arm.ArmActor
import at.ac.uibk.dps.smartfactory.actors.arm.ArmActorImpl
import at.ac.uibk.dps.smartfactory.actors.assemblycontroller.AssemblyControllerActorImpl
import at.ac.uibk.dps.smartfactory.actors.belt.BeltActorImpl
import at.ac.uibk.dps.smartfactory.actors.jobcontroller.JobControllerActor
import at.ac.uibk.dps.smartfactory.actors.jobcontroller.JobControllerActorImpl
import at.ac.uibk.dps.smartfactory.actors.messageprocessor.MessageProcessorImpl
import at.ac.uibk.dps.smartfactory.actors.monitor.MonitorActorImpl
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication class SmartFactory

fun main(args: Array<String>) {
  val role = System.getenv("ROLE")
  val actorId = System.getenv("ACTOR_ID") ?: "actor-0"

  println(role)
  when (role) {
    "jobcontroller" -> ActorRuntime.getInstance().registerActor(JobControllerActorImpl::class.java)
    "monitor" -> ActorRuntime.getInstance().registerActor(MonitorActorImpl::class.java)
    "messageprocessor" -> ActorRuntime.getInstance().registerActor(MessageProcessorImpl::class.java)
    "belt" -> ActorRuntime.getInstance().registerActor(BeltActorImpl::class.java)
    "arm" -> ActorRuntime.getInstance().registerActor(ArmActorImpl::class.java)
    "assemblycontroller" ->
      ActorRuntime.getInstance().registerActor(AssemblyControllerActorImpl::class.java)
    else -> println("ERROR: Unknown role $role")
  }

  runApplication<SmartFactory>(*args)

  when (role) {
    "jobcontroller" ->
      ActorProxyBuilder(JobControllerActor::class.java, ActorClient())
        .build(ActorId(actorId))
        .initialize()
    "arm" ->
      ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId)).initialize()
  }
}
