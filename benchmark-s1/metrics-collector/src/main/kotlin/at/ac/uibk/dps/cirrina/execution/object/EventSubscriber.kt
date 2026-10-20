package at.ac.uibk.dps.dapr.execution.`object`

import com.codahale.metrics.Meter
import io.dapr.Topic
import io.dapr.client.domain.CloudEvent
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class EventSubscriber {

  val partArrivals: Meter = metricsRegistry.meter("partArrivals")
  val partsCompleted: Meter = metricsRegistry.meter("partsCompleted")


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

    return ResponseEntity.ok().build()
  }
}
