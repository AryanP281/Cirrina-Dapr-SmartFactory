package ac.at.uibk.dps.dapr.smartfactory.actors.jobController

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
@ConditionalOnProperty("role", havingValue = "jobcontroller")
class JobControllerSubscriber :
  ApplicationListener<ApplicationReadyEvent> {

  private val actorId =
    System.getenv("ACTOR_ID") ?: "jobcontroller-0"

  private val proxy: JobControllerActor =
    ActorProxyBuilder(
      JobControllerActor::class.java,
      ActorClient()
    ).build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    proxy.initialize()
  }

  @Topic(name = "eProductComplete", pubsubName = "pubsub")
  @PostMapping("/eProductComplete")
  fun handleProductComplete(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    proxy.markProductCompleted()

    return ResponseEntity.ok().build()
  }
}