package at.ac.uibk.dps.dapr.execution.`object`

import com.codahale.metrics.Meter
import io.dapr.Topic
import io.dapr.client.domain.CloudEvent
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class EventSubscriber {

  val partAssemblyStartTimes = ConcurrentHashMap<Long, Long>()
  val assemblyControllerStartTimes = ConcurrentHashMap<Long, Long>()
  val beltMovementStartTimes = ConcurrentHashMap<Long,Long>()

  val partArrivals: Meter = metricsRegistry.meter("partArrivals")
  val partsCompleted: Meter = metricsRegistry.meter("partsCompleted")

  private val metricsDir = Path.of("./metrics")

  private val assemblyTimesFile = metricsDir.resolve("partAssemblyTimes.csv")
  val assemblyControllerTimesFile: Path = metricsDir.resolve("assemblyControllerTimes.csv")
  val beltMovementTimesFile: Path = metricsDir.resolve("beltMovementTimes.csv")

  private val assemblyTimesCsvLock = Any()
  val assemblyControllerTimesCsvLock = Any()
  val beltMovementTimesCsvLock = Any()

  init {
    Files.createDirectories(metricsDir)

    if (!Files.exists(assemblyTimesFile)) {
      Files.writeString(
        assemblyTimesFile,
        "partId,startTimeNs,endTimeNs,assemblyTimeNs\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
      )
    }

    if (!Files.exists(assemblyControllerTimesFile)) {
      Files.writeString(
        assemblyControllerTimesFile,
        "partId,startTimeNs,endTimeNs,assemblyControllerTimeNs\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
      )
    }

    if (!Files.exists(beltMovementTimesFile)) {
      Files.writeString(
        beltMovementTimesFile,
        "partId,startTimeNs,endTimeNs,beltMovementTimeNs\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
      )
    }
  }

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

      val assemblyTime = (emitTime - partAssemblyStartTime).coerceAtLeast(0L)

      synchronized(assemblyTimesCsvLock) {
        Files.writeString(
          assemblyTimesFile,
          "$partId,$partAssemblyStartTime,$emitTime,$assemblyTime\n",
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND,
        )
      }
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
    assemblyControllerStartTimes[partId] = (event.data["emittedTime"] as Number).toLong()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePartProcessingComplete", pubsubName = "pubsub")
  @PostMapping("/ePartProcessingComplete")
  fun ePartProcessingComplete(
    @RequestBody event: CloudEvent<Map<String, Any>>
  ): ResponseEntity<Unit> {

    val partId = (event.data["partId"] as Number).toLong()
    val partAssemblyStartTime = assemblyControllerStartTimes.remove(partId)

    if(partAssemblyStartTime != null) {
      val eventEmitTime = (event.data["emittedTime"] as Number).toLong()
      val assemblyControllerTime = (eventEmitTime - partAssemblyStartTime).coerceAtLeast(0L)

      synchronized(assemblyControllerTimesCsvLock) {
        Files.writeString(
          assemblyControllerTimesFile,
          "$partId,$partAssemblyStartTime,${eventEmitTime},$assemblyControllerTime\n",
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND,
        )
      }

    } else {
      logger.warn("No assembly start time found for part $partId")
    }

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePartMovementInitiated", pubsubName = "pubsub")
  @PostMapping("/ePartMovementInitiated")
  fun ePartMovementInitiated(
    @RequestBody event: CloudEvent<Map<String, Any>>
  ): ResponseEntity<Unit> {

    val partId = (event.data["partId"] as Number).toLong()

    beltMovementStartTimes[partId] = (event.data["emittedTime"] as Number).toLong()

    return ResponseEntity.ok().build()
  }

  @Topic(name = "ePartMovingComplete", pubsubName = "pubsub")
  @PostMapping("/ePartMovingComplete")
  fun ePartMovingComplete(
    @RequestBody event: CloudEvent<Map<String, Any>>
  ): ResponseEntity<Unit> {

    val partId = (event.data["partId"] as Number).toLong()
    val partMovementStartTime = beltMovementStartTimes.remove(partId)

    if(partMovementStartTime != null) {
      val eventEmitTime = (event.data["emittedTime"] as Number).toLong()
      val movementTime = (eventEmitTime - partMovementStartTime).coerceAtLeast(0L)

      synchronized(beltMovementTimesCsvLock) {
        Files.writeString(
          beltMovementTimesFile,
          "$partId,$partMovementStartTime,${eventEmitTime},$movementTime\n",
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND,
        )
      }

    } else {
      logger.warn("No movement start time found for part $partId")
    }

    return ResponseEntity.ok().build()
  }
}