package ac.at.uibk.dps.dapr.smartfactory.actors.arm

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
@ConditionalOnProperty("role", havingValue = "arm")
class ArmSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "arm-0"

  private val proxy: ArmActor =
    ActorProxyBuilder(ArmActor::class.java, ActorClient())
      .build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    proxy.initialize()
  }

  @Topic(name = "eArmPickup", pubsubName = "pubsub")
  @PostMapping("/eArmPickup")
  fun handleArmPickup(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.initiatePickup(
      (event.data["partId"] as Number).toLong()
    )

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eUpdatePickupStatus", pubsubName = "pubsub")
  @PostMapping("/eUpdatePickupStatus")
  fun handleUpdatePickupStatus(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.updatePickupStatus(
      event.data["success"]!! as Boolean
    )

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eCheckAssembleSuccess", pubsubName = "pubsub")
  @PostMapping("/eCheckAssembleSuccess")
  fun handleCheckAssembleSuccess(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.updateAssemblyStatus(
      event.data["success"]!! as Boolean
    )

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eResetArm", pubsubName = "pubsub")
  @PostMapping("/eResetArm")
  fun handleResetArm(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.armReset()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun handleJobDone(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.markJobDone()

    return ResponseEntity.ok().build()
  }
}