package at.ac.uibk.dps.smartfactory.actors.messageprocessor

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
@ConditionalOnProperty("role", havingValue = "messageprocessor")
class MessageProcessorSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
  private val actorProxy: MessageProcessorActor =
    ActorProxyBuilder(MessageProcessorActor::class.java, ActorClient()).build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {}

  @Topic(name = "eProcessMessage", pubsubName = "pubsub")
  @PostMapping("/eProcessMessage")
  fun eProcessMessage(@RequestBody event: CloudEvent<Map<String, String>>): ResponseEntity<Unit> {
    actorProxy.processMessage(event.data["msg"] ?: "")
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun eJobDone(): ResponseEntity<Unit> {
    actorProxy.markJobDone()
    return ResponseEntity.ok().build()
  }
}
