package at.ac.uibk.dps.smartfactory.actors.belt

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
@ConditionalOnProperty("app.role", havingValue = "belt")
class BeltSubscriber : ApplicationListener<ApplicationReadyEvent> {

    private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
    private val actorProxy : BeltActor = ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId(actorId))

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
    }

    @Topic(name = "eObjectValid", pubsubName = "pubsub")
    @PostMapping("/eObjectValid")
    fun eObjectValid(): ResponseEntity<Unit> {
        actorProxy.markObjectValidity()
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eStartUnload", pubsubName = "pubsub")
    @PostMapping("/eStartUnload")
    fun eStartUnload(): ResponseEntity<Unit> {
        actorProxy.startUnloading()
        return ResponseEntity.ok().build()
    }

    @Topic(name = "ePickedUp", pubsubName = "pubsub")
    @PostMapping("/ePickedUp")
    fun ePickedUp(): ResponseEntity<Unit> {
        actorProxy.markPickedUp()
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eJobDone", pubsubName = "pubsub")
    @PostMapping("/eJobDone")
    fun eJobDone(): ResponseEntity<Unit> {
        actorProxy.markJobDone()
        return ResponseEntity.ok().build()
    }
}