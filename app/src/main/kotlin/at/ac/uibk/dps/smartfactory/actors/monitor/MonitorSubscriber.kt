package at.ac.uibk.dps.smartfactory.actors.monitor

import at.ac.uibk.dps.smartfactory.metrics
import at.ac.uibk.dps.smartfactory.utils.Utils
import com.codahale.metrics.Timer
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import java.util.concurrent.TimeUnit
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

@RestController
@ConditionalOnProperty("role", havingValue = "monitor")
class MessageProcessorSubscriber {

  private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
  private val actorProxy: MonitorActor =
    ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId(actorId))

  private val eventTimer: Timer = metrics.timer("event.latency")
  private val processEventTimer: Timer = metrics.timer("processEvent.time")

  @Topic(name = "eScanned", pubsubName = "pubsub")
  @PostMapping("/eScanned")
  fun eScanned(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.markScanned()
    }

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
  @PostMapping("/eAssemblyComplete")
  fun eAssemblyComplete(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.markAssembled()
    }

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun eJobDone(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.markJobDone()
    }

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }
}
