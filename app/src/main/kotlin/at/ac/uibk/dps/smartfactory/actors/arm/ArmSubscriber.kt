package at.ac.uibk.dps.smartfactory.actors.arm

import at.ac.uibk.dps.smartfactory.metrics
import at.ac.uibk.dps.smartfactory.utils.Utils
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

@RestController
@ConditionalOnProperty("role", havingValue = "arm")
class ArmSubscriber : ApplicationListener<ApplicationReadyEvent> {

  private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"
  private val actorProxy: ArmActor =
    ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId))

  private val eventTimer: Timer = metrics.timer("event.latency")
  private val processEventTimer: Timer = metrics.timer("processEvent.time")

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    actorProxy.initialize()
  }

  @Topic(name = "eArmPickup", pubsubName = "pubsub")
  @PostMapping("/eArmPickup")
  fun handleArmPickup(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.initiatePickup()
    }

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eMarkPickedUp", pubsubName = "pubsub")
  @PostMapping("/eMarkPickedUp")
  fun handleMarkPickedUp(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.markPickedUp()
    }

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eUpdatePickupStatus", pubsubName = "pubsub")
  @PostMapping("/eUpdatePickupStatus")
  fun handleUpdatePickupStatus(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.updatePickupStatus(event.data["success"]!! as Boolean)
    }

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
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.updateAssemblyStatus(event.data["success"]!! as Boolean)
    }

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eResetArm", pubsubName = "pubsub")
  @PostMapping("/eResetArm")
  fun handleResetArm(@RequestBody event: CloudEvent<Map<String, Any?>>): ResponseEntity<Unit> {
    val delta = measureTime {
      // Logging event latency
      val eventEmitTime = event.data["emittedTime"]!! as Long
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.armReset()
    }

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
      val deltaTime: Long = (Utils.getCurrentTimeNs() - eventEmitTime).coerceAtLeast(0)
      eventTimer.update(deltaTime, TimeUnit.NANOSECONDS)

      actorProxy.markJobDone()
    }

    // Logging event processing time
    processEventTimer.update(delta.toJavaDuration())

    return ResponseEntity.ok().build()
  }
}
