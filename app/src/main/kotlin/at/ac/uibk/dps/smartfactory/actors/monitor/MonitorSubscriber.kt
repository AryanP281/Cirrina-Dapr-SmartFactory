package at.ac.uibk.dps.smartfactory.actors.monitor

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "monitor")
class MessageProcessorSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
  private val actorProxy: MonitorActor =
    ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {}

  @Topic(name = "eProductComplete", pubsubName = "pubsub")
  @PostMapping("/eProductComplete")
  fun eProductComplete(): ResponseEntity<Unit> {
    actorProxy.incrementProductsCompletedCount()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eScanned", pubsubName = "pubsub")
  @PostMapping("/eScanned")
  fun eScanned(): ResponseEntity<Unit> {
    actorProxy.markScanned()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
  @PostMapping("/eAssemblyComplete")
  fun eAssemblyComplete(): ResponseEntity<Unit> {
    actorProxy.markAssembled()
    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun eJobDone(): ResponseEntity<Unit> {
    actorProxy.markJobDone()
    return ResponseEntity.ok().build()
  }
}
