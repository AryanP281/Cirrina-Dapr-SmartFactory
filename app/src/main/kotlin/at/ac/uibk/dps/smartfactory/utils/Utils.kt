package at.ac.uibk.dps.smartfactory.utils

import io.dapr.client.DaprClient
import reactor.core.publisher.Mono
import kotlin.time.Clock

object Utils {
  fun publishEvent(daprClient: DaprClient, pubsubName: String = "pubsub", eventTopic : String, payload : MutableMap<String, Any?>) : Mono<Void> {
    val emitTime = getEmittedTimeNs()
    payload["emittedTime"] = emitTime

    return daprClient.publishEvent(pubsubName, eventTopic, payload)
  }

  fun getEmittedTimeNs(): Long {
    val now = Clock.System.now()

    return now.epochSeconds * 1_000_000_000L + now.nanosecondsOfSecond
  }
}
