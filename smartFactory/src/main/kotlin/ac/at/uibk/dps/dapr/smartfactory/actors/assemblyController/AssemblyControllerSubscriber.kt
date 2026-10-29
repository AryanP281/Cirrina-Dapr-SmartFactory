package ac.at.uibk.dps.dapr.smartfactory.actors.assemblyController

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import java.util.Base64
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("role", havingValue = "assemblycontroller")
class AssemblyControllerSubscriber :
  ApplicationListener<ApplicationReadyEvent> {

  private val actorId =
    System.getenv("ACTOR_ID") ?: "assemblycontroller-0"

  private val proxy: AssemblyControllerActor =
    ActorProxyBuilder(
      AssemblyControllerActor::class.java,
      ActorClient()
    ).build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    proxy.initialize()
  }

  @Topic(name = "eBeamInterruptedStart", pubsubName = "pubsub")
  @PostMapping("/eBeamInterruptedStart")
  fun handleBeamInterruptedStart(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.detectedAtStart()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePhotoCaptured", pubsubName = "pubsub")
  @PostMapping("/ePhotoCaptured")
  fun handlePhotoCaptured(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.processCapturedPhoto(
      Base64.getDecoder().decode(
        event.data["data"] as? String ?: ""
      )
    )

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePhotoScanned", pubsubName = "pubsub")
  @PostMapping("/ePhotoScanned")
  fun handlePhotoScanned(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.processPhotoScan(
      event.data["validObject"] as? Boolean ?: false
    )

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eObjectDiscarded", pubsubName = "pubsub")
  @PostMapping("/eObjectDiscarded")
  fun handleObjectDiscarded(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.objectDiscarded()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eBeamInterruptedEnd", pubsubName = "pubsub")
  @PostMapping("/eBeamInterruptedEnd")
  fun handleBeamInterruptedEnd(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.detectedAtEnd()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePickedUp", pubsubName = "pubsub")
  @PostMapping("/ePickedUp")
  fun handlePickedUp(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.processPickup()

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