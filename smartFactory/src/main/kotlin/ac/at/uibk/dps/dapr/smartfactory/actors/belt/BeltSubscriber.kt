package ac.at.uibk.dps.dapr.smartfactory.actors.belt

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("role", havingValue = "belt")
class BeltSubscriber {

  private val actorId =
    System.getenv("ACTOR_ID") ?: "belt-0"

  private val proxy: BeltActor =
    ActorProxyBuilder(
      BeltActor::class.java,
      ActorClient()
    ).build(ActorId(actorId))

  @Topic(name = "eObjectValid", pubsubName = "pubsub")
  @PostMapping("/eObjectValid")
  fun handleObjectValid(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.markObjectValidity()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eStartUnload", pubsubName = "pubsub")
  @PostMapping("/eStartUnload")
  fun handleStartUnload(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.startUnloading(
      (event.data["partId"] as Number).toLong()
    )

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePickedUp", pubsubName = "pubsub")
  @PostMapping("/ePickedUp")
  fun handlePickedUp(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.markPickedUp()

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