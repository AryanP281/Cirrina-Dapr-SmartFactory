package at.ac.uibk.dps.smartfactory.actors.monitor

import at.ac.uibk.dps.smartfactory.api.StatisticsRequest
import at.ac.uibk.dps.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class MonitorActorImpl(runtimeContext: ActorRuntimeContext<MonitorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), MonitorActor {

  enum class State {
    MONITORING,
    JOB_DONE,
  }

  private var currentActiveState: State = State.MONITORING
  private var nScans = 0
  private var nAssemblies = 0

  private val daprClient = DaprClientBuilder().build()

  override fun markScanned() {
    if (currentActiveState == State.MONITORING) {
      nScans += 1

      sendStatistics()
    }
  }

  override fun markAssembled() {
    if (currentActiveState == State.MONITORING) {
      nAssemblies += 1

      sendStatistics()
    }
  }

  override fun markJobDone() {
    if (currentActiveState == State.MONITORING) {
      currentActiveState = State.JOB_DONE

      sendStatistics()
    }
  }

  private fun sendStatistics() {
    val jobDone =
      daprClient.getState("statestore", "isJobDone", Boolean::class.java).block()?.value ?: false
    val productsCompleted =
      daprClient.getState("statestore", "productsCompleted", Int::class.java).block()?.value ?: 0

    // Invoke SendStatistics service
    Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone))
      .subscribe()
  }
}
