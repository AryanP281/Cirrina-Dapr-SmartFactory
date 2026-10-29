package ac.at.uibk.dps.dapr.smartfactory.actors.arm

import ac.at.uibk.dps.dapr.smartfactory.metrics
import ac.at.uibk.dps.dapr.smartfactory.utils.Utils
import com.codahale.metrics.Timer
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
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
import java.util.concurrent.ConcurrentHashMap

@RestController
@ConditionalOnProperty("role", havingValue = "arm")
class ArmSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "arm-0"
  private val proxy: ArmActor =
    ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId))

  private val eventTimer: Timer = metrics.timer("event.latency")
  private val processEventTimer: Timer = metrics.timer("processEvent.time")
  private val eventHandlingTimer: Timer = metrics.timer("event.handling")
  private val perEventTimers = ConcurrentHashMap<String, ArrayList<Timer>>()

  override fun onApplicationEvent(event: ApplicationReadyEvent) {

    val inputEvents = arrayOf("eArmPickup", "eUpdatePickupStatus", "eCheckAssembleSuccess", "eResetArm", "eJobDone")
    for(inputEvent in inputEvents) {
      perEventTimers.set(inputEvent, arrayListOf(metrics.timer("$inputEvent.event.latency"),
        metrics.timer("$inputEvent.processEvent.time"),
        metrics.timer("$inputEvent.event.handling"),))
    }

    proxy.initialize()
  }

  @Topic(name = "eArmPickup", pubsubName = "pubsub")
  @PostMapping("/eArmPickup")
  fun handleArmPickup(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)
      perEventTimers["eArmPickup"]
        ?.get(0)
        ?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.initiatePickup((event.data["partId"] as Number).toLong())
    }

    //Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)
    perEventTimers["eArmPickup"]
      ?.get(1)
      ?.update(delta.toJavaDuration())
    perEventTimers["eArmPickup"]
      ?.get(2)
      ?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eUpdatePickupStatus", pubsubName = "pubsub")
  @PostMapping("/eUpdatePickupStatus")
  fun handleUpdatePickupStatus(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime =
        (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eUpdatePickupStatus"]
        ?.get(0)
        ?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.updatePickupStatus(event.data["success"]!! as Boolean)
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime =
      (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eUpdatePickupStatus"]
      ?.get(1)
      ?.update(delta.toJavaDuration())

    perEventTimers["eUpdatePickupStatus"]
      ?.get(2)
      ?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eCheckAssembleSuccess", pubsubName = "pubsub")
  @PostMapping("/eCheckAssembleSuccess")
  fun handleCheckAssembleSuccess(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime =
        (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eCheckAssembleSuccess"]
        ?.get(0)
        ?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.updateAssemblyStatus(event.data["success"]!! as Boolean)
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime =
      (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eCheckAssembleSuccess"]
      ?.get(1)
      ?.update(delta.toJavaDuration())

    perEventTimers["eCheckAssembleSuccess"]
      ?.get(2)
      ?.update(deltaTime, TimeUnit.NANOSECONDS)

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eResetArm", pubsubName = "pubsub")
  @PostMapping("/eResetArm")
  fun handleResetArm(
    @RequestBody event: CloudEvent<Map<String, Any?>>
  ): ResponseEntity<Unit> {

    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime =
        (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      perEventTimers["eResetArm"]
        ?.get(0)
        ?.update(deltaTime, TimeUnit.NANOSECONDS)

      proxy.armReset()
    }

    // Logging handling time
    val eventEmitTime = event.data["emittedTime"]!! as Long
    val deltaTime =
      (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)

    eventHandlingTimer.update(deltaTime, TimeUnit.NANOSECONDS)

    perEventTimers["eResetArm"]
      ?.get(1)
      ?.update(delta.toJavaDuration())

    perEventTimers["eResetArm"]
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
