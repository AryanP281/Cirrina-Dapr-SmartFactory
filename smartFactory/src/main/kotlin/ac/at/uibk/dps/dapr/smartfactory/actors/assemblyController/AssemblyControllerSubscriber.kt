package ac.at.uibk.dps.dapr.smartfactory.actors.assemblyController

import ac.at.uibk.dps.dapr.smartfactory.metrics
import ac.at.uibk.dps.dapr.smartfactory.utils.Utils
import com.codahale.metrics.Timer
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.Base64
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
@ConditionalOnProperty("role", havingValue = "assemblycontroller")
class AssemblyControllerSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "assemblycontroller-0"

  private val proxy: AssemblyControllerActor =
    ActorProxyBuilder(AssemblyControllerActor::class.java, ActorClient()).build(ActorId(actorId))

  private val eventTimer: Timer = metrics.timer("event.latency")
  private val processEventTimer: Timer = metrics.timer("processEvent.time")
  private val eventHandlingTimer: Timer = metrics.timer("event.handling")

  private val perEventTimers = ConcurrentHashMap<String, ArrayList<Timer>>()

  private val photoCaptureEventTimesFile: Path =
    Path.of("./metrics").resolve("photoCaptureEventTimes.csv")
  private val photoCaptureEventTimesCsvLock = Any()

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    val inputEvents =
      arrayOf(
        "eBeamInterruptedStart",
        "ePhotoCaptured",
        "ePhotoScanned",
        "eObjectDiscarded",
        "eBeamInterruptedEnd",
        "ePickedUp",
        "eJobDone",
      )

    for (inputEvent in inputEvents) {
      perEventTimers[inputEvent] =
        arrayListOf(
          metrics.timer("$inputEvent.event.latency"),
          metrics.timer("$inputEvent.processEvent.time"),
          metrics.timer("$inputEvent.event.handling"),
        )
    }

    if (!Files.exists(photoCaptureEventTimesFile)) {
      Files.writeString(
        photoCaptureEventTimesFile,
        "cloudEventId,eventId,receiveTimeNs\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
      )
    }

    proxy.initialize()
  }

  @Topic(name = "eBeamInterruptedStart", pubsubName = "pubsub")
  @PostMapping("/eBeamInterruptedStart")
  fun handleBeamInterruptedStart(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eBeamInterruptedStart"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.detectedAtStart()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eBeamInterruptedStart"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["eBeamInterruptedStart"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePhotoCaptured", pubsubName = "pubsub")
  @PostMapping("/ePhotoCaptured")
  fun handlePhotoCaptured(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {

    val eventReceiveTime = Utils.getCurrentTimeNs()

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (eventReceiveTime - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["ePhotoCaptured"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.processCapturedPhoto(Base64.getDecoder().decode(event.data["data"] as? String ?: ""))
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["ePhotoCaptured"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["ePhotoCaptured"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    synchronized(photoCaptureEventTimesCsvLock) {
      Files.writeString(
        photoCaptureEventTimesFile,
        "${event.id},${(event.data["eId"] as Number).toLong()},$eventReceiveTime\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND,
      )
    }

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePhotoScanned", pubsubName = "pubsub")
  @PostMapping("/ePhotoScanned")
  fun handlePhotoScanned(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["ePhotoScanned"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.processPhotoScan(event.data["validObject"] as? Boolean ?: false)
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["ePhotoScanned"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["ePhotoScanned"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eObjectDiscarded", pubsubName = "pubsub")
  @PostMapping("/eObjectDiscarded")
  fun handleObjectDiscarded(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eObjectDiscarded"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.objectDiscarded()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eObjectDiscarded"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["eObjectDiscarded"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eBeamInterruptedEnd", pubsubName = "pubsub")
  @PostMapping("/eBeamInterruptedEnd")
  fun handleBeamInterruptedEnd(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eBeamInterruptedEnd"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.detectedAtEnd()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eBeamInterruptedEnd"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["eBeamInterruptedEnd"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePickedUp", pubsubName = "pubsub")
  @PostMapping("/ePickedUp")
  fun handlePickedUp(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["ePickedUp"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.processPickup()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["ePickedUp"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["ePickedUp"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eJobDone", pubsubName = "pubsub")
  @PostMapping("/eJobDone")
  fun handleJobDone(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eJobDone"]?.get(0)?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.markJobDone()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eJobDone"]?.get(1)?.update(delta.toJavaDuration())

    perEventTimers["eJobDone"]?.get(2)?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }
}
