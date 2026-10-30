package ac.at.uibk.dps.dapr.smartfactory.actors.jobController

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
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("role", havingValue = "jobcontroller")
class JobControllerSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "jobcontroller-0"

  private val proxy: JobControllerActor =
    ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId(actorId))

  private val eventTimer: Timer = metrics.timer("event.latency")
  private val processEventTimer: Timer = metrics.timer("processEvent.time")
  private val eventHandlingTimer: Timer = metrics.timer("event.handling")

  private val perEventTimers = ConcurrentHashMap<String, ArrayList<Timer>>()

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    val inputEvents = arrayOf("eProductComplete")

    for (inputEvent in inputEvents) {
      perEventTimers[inputEvent] =
        arrayListOf(
          metrics.timer("$inputEvent.event.latency"),
          metrics.timer("$inputEvent.processEvent.time"),
          metrics.timer("$inputEvent.event.handling"),
        )
    }

    proxy.initialize()
  }

  @Topic(name = "eProductComplete", pubsubName = "pubsub")
  @PostMapping("/eProductComplete")
  fun handleProductComplete(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eProductComplete"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.markProductCompleted()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eProductComplete"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["eProductComplete"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }
}
