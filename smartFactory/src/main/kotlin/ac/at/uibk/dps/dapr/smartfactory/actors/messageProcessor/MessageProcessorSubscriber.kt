package ac.at.uibk.dps.dapr.smartfactory.actors.messageProcessor

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
@ConditionalOnProperty("role", havingValue = "messageprocessor")
class MessageProcessorSubscriber {

  private val actorId =
    System.getenv("ACTOR_ID") ?: "messageprocessor-0"

  private val proxy: MessageProcessorActor =
    ActorProxyBuilder(
      MessageProcessorActor::class.java,
      ActorClient()
    ).build(ActorId(actorId))

  @Topic(name = "eProcessMessage", pubsubName = "pubsub")
  @PostMapping("/eProcessMessage")
  fun handleProcessMessage(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.processMessage(
      event.data["msg"] as? String ?: ""
    )

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