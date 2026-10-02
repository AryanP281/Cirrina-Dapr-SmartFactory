package at.ac.uibk.dps.smartfactory.actors.belt

import at.ac.uibk.dps.smartfactory.services.Services
import at.ac.uibk.dps.smartfactory.utils.Utils
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.time.Duration
import reactor.core.publisher.Mono

class BeltActorImpl(runtimeContext: ActorRuntimeContext<BeltActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), BeltActor {

  enum class State {
    LOADING,
    TRANSPORTING,
    UNLOADING,
    JOB_DONE,
  }

  private var currentActiveState: State = State.LOADING

  private val daprClient = DaprClientBuilder().build()

  private fun transition(targetState: State, data: Any? = null) {
    when (targetState) {
      State.LOADING -> {
        if (currentActiveState == State.UNLOADING) {
          // Exit actions
          unregisterTimer("armPickupTimeout-${id}").subscribe()

          currentActiveState = targetState
        }
      }

      State.TRANSPORTING -> {
        if (currentActiveState == State.LOADING) {
          currentActiveState = State.TRANSPORTING
          transportingState()
        }
      }

      State.UNLOADING -> {
        if (currentActiveState == State.TRANSPORTING) {
          // Exit actions
          Services.stopBelt().subscribe()

          currentActiveState = State.UNLOADING
          unloadingState()
        }
      }

      State.JOB_DONE -> {
        // Exit actions
        if (currentActiveState == State.UNLOADING)
          unregisterTimer("armPickupTimeout-${id}").subscribe()

        currentActiveState = State.JOB_DONE
      }
    }
  }

  private fun transportingState() {
    // Invoke MoveBelt Action
    Services.moveBelt().subscribe()
  }

  private fun unloadingState() {
    // Starting timer for eArmPickup
    registerActorTimer(
        "armPickupTimeout-${id}",
        "armPickupTimeout",
        0,
        Duration.ofSeconds(0),
        Duration.ofSeconds(1),
      )
      .subscribe()
  }

  override fun markObjectValidity() {
    transition(State.TRANSPORTING)
  }

  override fun startUnloading() {
    transition(State.UNLOADING)
  }

  override fun markJobDone() {
    transition(State.JOB_DONE)
  }

  override fun markPickedUp() {
    transition(State.LOADING)
  }

  override fun armPickupTimeout(): Mono<Void> {
    if (currentActiveState == State.UNLOADING) {
      // Raising eArmPickup
      Utils.publishEvent(daprClient, "pubsub", "eArmPickup", mutableMapOf<String, Any?>())
        .subscribe()
    }

    return Mono.empty()
  }
}
