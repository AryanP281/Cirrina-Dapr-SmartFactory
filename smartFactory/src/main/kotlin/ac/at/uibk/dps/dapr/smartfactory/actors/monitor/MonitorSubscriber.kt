package ac.at.uibk.dps.dapr.smartfactory.actors.monitor

import ac.at.uibk.dps.dapr.smartfactory.metrics
import ac.at.uibk.dps.dapr.smartfactory.utils.Utils
import com.codahale.metrics.Timer
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.time.measureTime
import kotlin.time.toJavaDuration
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("role", havingValue = "monitor")
class MonitorSubscriber {

  private val actorId = System.getenv("ACTOR_ID") ?: "monitor-0"

  private val proxy: MonitorActor =
    ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId(actorId))

  private val eventTimer: Timer = metrics.timer("event.latency")
  private val processEventTimer: Timer = metrics.timer("processEvent.time")
  private val eventHandlingTimer: Timer = metrics.timer("event.handling")

  private val perEventTimers =
    ConcurrentHashMap<String, ArrayList<Timer>>().apply {
      val inputEvents =
        arrayOf(
          "eScanned",
          "eAssemblyComplete",
          "eJobDone",
        )

      for (inputEvent in inputEvents) {
        this[inputEvent] =
          arrayListOf(
            metrics.timer("$inputEvent.event.latency"),
            metrics.timer("$inputEvent.processEvent.time"),
            metrics.timer("$inputEvent.event.handling"),
          )
      }
    }

  @Topic(name = "eScanned", pubsubName = "pubsub")
  @PostMapping("/eScanned")
  fun handleScanned(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime =
        (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eScanned"]
        ?.get(0)
        ?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.markScanned()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime =
      (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eScanned"]
      ?.get(1)
      ?.update(delta.toJavaDuration())

    perEventTimers["eScanned"]
      ?.get(2)
      ?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
  @PostMapping("/eAssemblyComplete")
  fun handleAssemblyComplete(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime =
        (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eAssemblyComplete"]
        ?.get(0)
        ?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.markAssembled()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime =
      (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eAssemblyComplete"]
      ?.get(1)
      ?.update(delta.toJavaDuration())

    perEventTimers["eAssemblyComplete"]
      ?.get(2)
      ?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun handleJobDone(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime =
        (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eJobDone"]
        ?.get(0)
        ?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.markJobDone()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime =
      (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eJobDone"]
      ?.get(1)
      ?.update(delta.toJavaDuration())

    perEventTimers["eJobDone"]
      ?.get(2)
      ?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }
}