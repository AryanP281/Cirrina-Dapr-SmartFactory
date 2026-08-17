package at.ac.uibk.dps.smartfactory.actors.assemblycontroller

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
@ConditionalOnProperty("role", havingValue = "assemblycontroller")
class AssemblyControllerSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
  private val actorProxy: AssemblyControllerActor =
    ActorProxyBuilder(AssemblyControllerActor::class.java, ActorClient()).build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    actorProxy.initialize()
  }

  @Topic(name = "eBeamInterruptedStart", pubsubName = "pubsub")
  @PostMapping("/eBeamInterruptedStart")
  fun eBeamInterruptedStart(): ResponseEntity<Unit> {
    actorProxy.detectedAtStart()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePhotoCaptured", pubsubName = "pubsub")
  @PostMapping("/ePhotoCaptured")
  fun ePhotoCaptured(@RequestBody event: CloudEvent<Map<String, ByteArray>>): ResponseEntity<Unit> {
    actorProxy.processCapturedPhoto(event.data["data"] ?: byteArrayOf())
    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePhotoScanned", pubsubName = "pubsub")
  @PostMapping("/ePhotoScanned")
  fun ePhotoScanned(@RequestBody event: CloudEvent<Map<String, Boolean>>): ResponseEntity<Unit> {
    actorProxy.processPhotoScan(event.data["validObject"] ?: false)
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eObjectDiscarded", pubsubName = "pubsub")
  @PostMapping("/eObjectDiscarded")
  fun eObjectDiscarded(): ResponseEntity<Unit> {
    actorProxy.objectDiscarded()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eBeamInterruptedEnd", pubsubName = "pubsub")
  @PostMapping("/eBeamInterruptedEnd")
  fun eBeamInterruptedEnd(): ResponseEntity<Unit> {
    actorProxy.detectedAtEnd()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePickedUp", pubsubName = "pubsub")
  @PostMapping("/ePickedUp")
  fun ePickedUp(): ResponseEntity<Unit> {
    actorProxy.processPickup()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun eJobDone(): ResponseEntity<Unit> {
    actorProxy.markJobDone()
    return ResponseEntity.ok().build()
  }
}
