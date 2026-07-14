package at.ac.uibk.dps.smartfactory.actors.jobcontroller

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
@ConditionalOnProperty("app.role", havingValue = "jobcontroller")
class JobControllerSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
  private val actorProxy: JobControllerActor =
    ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId(actorId))

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    actorProxy.initialize()
  }

  @Topic(name = "eProductComplete", pubsubName = "pubsub")
  @PostMapping("/eProductComplete")
  fun eProductComplete(): ResponseEntity<Unit> {
    actorProxy.markProductCompleted()
    return ResponseEntity.ok().build()
  }
}
