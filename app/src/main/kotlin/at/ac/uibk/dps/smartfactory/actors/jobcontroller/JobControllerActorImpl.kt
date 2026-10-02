package at.ac.uibk.dps.smartfactory.actors.jobcontroller

import at.ac.uibk.dps.smartfactory.utils.Utils
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class JobControllerActorImpl(
  runtimeContext: ActorRuntimeContext<JobControllerActorImpl>,
  id: ActorId,
) : AbstractActor(runtimeContext, id), JobControllerActor {

  enum class State {
    STARTING,
    RUNNING,
    JOB_DONE,
  }

  private val totalProducts = 1
  private var currActiveState: State = State.STARTING

  private val daprClient = DaprClientBuilder().build()

  override fun initialize() {
    transition(State.STARTING)
  }

  override fun markProductCompleted() {
    if (currActiveState == State.RUNNING) {
      daprClient.getState("statestore", "productsCompleted", Int::class.java).block()?.value.let {
        daprClient.saveState("statestore", "productsCompleted", (it ?: 0) + 1).block()
      }
      this.checkJobDone()
    }
  }

  private fun checkJobDone() {
    val productsCompleted =
      daprClient.getState("statestore", "productsCompleted", Int::class.java).block()?.value ?: 0
    if (productsCompleted >= totalProducts) transition(State.JOB_DONE)
  }

  private fun transition(targetState: State) {
    if (currActiveState == State.JOB_DONE) // Terminal state
     return

    when (targetState) {
      State.STARTING -> {
        currActiveState = State.STARTING
        startingState()
      }
      State.RUNNING -> {
        if (currActiveState == State.STARTING)
          currActiveState = State.RUNNING
      }
      State.JOB_DONE -> {
        if (currActiveState == State.RUNNING) {
          currActiveState = State.JOB_DONE
          jobDoneState()
        }
      }
    }
  }

  private fun startingState() {
    // Emit event to Message Processor
    Utils.publishEvent(
        daprClient,
        "pubsub",
        "eProcessMessage",
        mutableMapOf("msg" to "Job started..."),
      )
      .subscribe()

    // Transition to running state
    transition(State.RUNNING)
  }

  private fun jobDoneState() {
    // Emit event to Message Processor
    Utils.publishEvent(
        daprClient,
        "pubsub",
        "eProcessMessage",
        mutableMapOf("msg" to "Job done..."),
      )
      .subscribe()

    // Updating job done status
    daprClient.saveState("statestore", "isJobDone", true).block()

    // Emit JobDone
    Utils.publishEvent(daprClient, "pubsub", "eJobDone", mutableMapOf()).subscribe()
  }
}
