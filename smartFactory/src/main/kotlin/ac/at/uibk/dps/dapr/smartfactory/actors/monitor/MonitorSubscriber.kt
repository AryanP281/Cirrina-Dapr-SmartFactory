package ac.at.uibk.dps.dapr.smartfactory.actors.monitor

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
@ConditionalOnProperty("role", havingValue = "monitor")
class MonitorSubscriber {

  private val actorId =
    System.getenv("ACTOR_ID") ?: "monitor-0"

  private val proxy: MonitorActor =
    ActorProxyBuilder(
      MonitorActor::class.java,
      ActorClient()
    ).build(ActorId(actorId))

  @Topic(name = "eScanned", pubsubName = "pubsub")
  @PostMapping("/eScanned")
  fun handleScanned(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.markScanned()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
  @PostMapping("/eAssemblyComplete")
  fun handleAssemblyComplete(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.markAssembled()

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