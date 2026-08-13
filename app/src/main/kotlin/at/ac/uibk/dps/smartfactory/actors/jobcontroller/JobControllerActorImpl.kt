package at.ac.uibk.dps.smartfactory.actors.jobcontroller

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class JobControllerActorImpl(
  runtimeContext: ActorRuntimeContext<JobControllerActorImpl>,
  id: ActorId,
) : AbstractActor(runtimeContext, id), JobControllerActor {
  private val totalProducts = 10
  private var currActiveState: JobControllerActor.States = JobControllerActor.States.STARTING

  private val daprClient = DaprClientBuilder().build()

  override fun initialize() {
    transition(JobControllerActor.States.STARTING)
  }

  override fun markProductCompleted() {
    if (currActiveState == JobControllerActor.States.RUNNING) {
      daprClient.getState("statestore","productsCompleted", Int::class.java).block()?.value.let {
        daprClient.saveState("statestore", "productsCompleted", (it ?: 0)+1).block()
      }
      this.checkJobDone()
    }
  }

  private fun checkJobDone() {
    val productsCompleted = daprClient.getState("statestore","productsCompleted", Int::class.java).block()?.value ?: 0
    if (productsCompleted >= totalProducts) transition(JobControllerActor.States.JOB_DONE)
  }

  private fun transition(targetState: JobControllerActor.States) {
    if (currActiveState == JobControllerActor.States.JOB_DONE) // Terminal state
     return

    when (targetState) {
      JobControllerActor.States.STARTING -> {
        currActiveState = JobControllerActor.States.STARTING
        startingState()
      }
      JobControllerActor.States.RUNNING -> {
        if (currActiveState == JobControllerActor.States.STARTING)
          currActiveState = JobControllerActor.States.RUNNING
      }
      JobControllerActor.States.JOB_DONE -> {
        if (currActiveState == JobControllerActor.States.RUNNING) {
          currActiveState = JobControllerActor.States.JOB_DONE
          jobDoneState()
        }
      }
    }
  }

  private fun startingState() {
    // Emit event to Message Processor
    daprClient
      .publishEvent("pubsub", "eProcessMessage", mapOf("msg" to "Job started..."))
      .subscribe()

    // Transition to running state
    transition(JobControllerActor.States.RUNNING)
  }

  private fun jobDoneState() {
    //Updating job done status
    daprClient.saveState("statestore", "isJobDone", true).block()

    // Emit event to Message Processor
    daprClient.publishEvent("pubsub", "eProcessMessage", mapOf("msg" to "Job done...")).subscribe()

    // Emit JobDone
    daprClient.publishEvent("pubsub", "eJobDone", mapOf<String, Any>()).subscribe()
  }
}
