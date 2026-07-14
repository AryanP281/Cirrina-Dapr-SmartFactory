package at.ac.uibk.dps.smartfactory.actors.arm

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "arm")
class ArmSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
  private val actorProxy: ArmActor =
    ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    actorProxy.initialize()
  }

  @Topic(name = "eArmPickup", pubsubName = "pubsub")
  @PostMapping("/eArmPickup")
  fun eArmPickup(): ResponseEntity<Unit> {
    actorProxy.initiatePickup()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eMarkPickedUp", pubsubName = "pubsub")
  @PostMapping("/eMarkPickedUp")
  fun eMarkPickedUp(): ResponseEntity<Unit> {
    actorProxy.markPickedUp()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eUpdatePickupStatus", pubsubName = "pubsub")
  @PostMapping("/eUpdatePickupStatus")
  fun eUpdatePickupStatus(@RequestBody event: CloudEvent<Boolean>): ResponseEntity<Unit> {
    actorProxy.updatePickupStatus(event.data)
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eCheckAssembleSuccess", pubsubName = "pubsub")
  @PostMapping("/eCheckAssembleSuccess")
  fun eCheckAssembleSuccess(@RequestBody event: CloudEvent<Boolean>): ResponseEntity<Unit> {
    actorProxy.updateAssemblyStatus(event.data)
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eResetArm", pubsubName = "pubsub")
  @PostMapping("/eResetArm")
  fun eResetArm(): ResponseEntity<Unit> {
    actorProxy.armReset()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun eJobDone(): ResponseEntity<Unit> {
    actorProxy.markJobDone()
    return ResponseEntity.ok().build()
  }
}
