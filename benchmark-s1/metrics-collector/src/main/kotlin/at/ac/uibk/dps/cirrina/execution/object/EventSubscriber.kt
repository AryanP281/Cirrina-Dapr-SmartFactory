package at.ac.uibk.dps.dapr.execution.`object`

import com.codahale.metrics.Meter
import com.codahale.metrics.Timer
import io.dapr.Topic
import io.dapr.client.domain.CloudEvent
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class EventSubscriber {

  val partArrivals: Meter = metricsRegistry.meter("partArrivals")
  val partsCompleted: Meter = metricsRegistry.meter("partsCompleted")
  val partAssemblyTimes: Timer = metricsRegistry.timer("partAssemblyTimes")

  val partAssemblyStartTimes = ConcurrentHashMap<Long, Long>()

  @Topic(name = "eBeamInterruptedStart", pubsubName = "pubsub")
  @PostMapping("/eBeamInterruptedStart")
  fun eBeamInterruptedStart(): ResponseEntity<Unit> {
    partArrivals.mark()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
  @PostMapping("/eAssemblyComplete")
  fun eAssemblyComplete(@RequestBody event: CloudEvent<Map<String, Any>>): ResponseEntity<Unit> {
    partsCompleted.mark()

    val partId = (event.data["partId"] as Number).toLong()
    val partAssemblyStartTime = partAssemblyStartTimes.remove(partId)

    if (partAssemblyStartTime != null) {
      val emitTime = (event.data["emittedTime"] as Number).toLong()
      val assemblyTime = (emitTime - partAssemblyStartTime).coerceAtLeast(0)
      partAssemblyTimes.update(assemblyTime, TimeUnit.NANOSECONDS)
    }

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePartAssemblyInitiated", pubsubName = "pubsub")
  @PostMapping("/ePartAssemblyInitiated")
  fun ePartAssemblyInitiated(
    @RequestBody event: CloudEvent<Map<String, Any>>
  ): ResponseEntity<Unit> {

    val partId = (event.data["partId"] as Number).toLong()
    partAssemblyStartTimes[partId] = (event.data["emittedTime"] as Number).toLong()

    return ResponseEntity.ok().build()
  }
}
