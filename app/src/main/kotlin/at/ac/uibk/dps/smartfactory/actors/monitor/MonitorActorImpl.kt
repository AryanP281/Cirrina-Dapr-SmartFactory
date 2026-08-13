package at.ac.uibk.dps.smartfactory.actors.monitor

import at.ac.uibk.dps.smartfactory.api.StatisticsRequest
import at.ac.uibk.dps.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import org.slf4j.LoggerFactory

class MonitorActorImpl(runtimeContext: ActorRuntimeContext<MonitorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), MonitorActor {

  private var currentActiveState: MonitorActor.States = MonitorActor.States.MONITORING
  private var nScans = 0
  private var nAssemblies = 0

  private val daprClient = DaprClientBuilder().build()

  private val logger = LoggerFactory.getLogger(MonitorActorImpl::class.java)

  override fun markScanned() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nScans += 1

      sendStatistics()
    }
  }

  override fun markAssembled() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      nAssemblies += 1

      sendStatistics()
    }
  }

  override fun markJobDone() {
    if (currentActiveState == MonitorActor.States.MONITORING) {
      currentActiveState = MonitorActor.States.JOB_DONE

      sendStatistics()
    }
  }

  private fun sendStatistics()
  {
    val jobDone = daprClient.getState("statestore", "isJobDone", Boolean::class.java).block()?.value ?: false
    val productsCompleted = daprClient.getState("statestore", "productsCompleted", Int::class.java).block()?.value ?: 0

    // Invoke SendStatistics service
    Services.sendStatistics(StatisticsRequest(nScans, nAssemblies, productsCompleted, jobDone))
      .subscribe()
  }
}
