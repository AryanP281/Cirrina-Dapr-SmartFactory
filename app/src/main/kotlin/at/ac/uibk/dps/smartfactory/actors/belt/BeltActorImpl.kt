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

  private var currentActiveState: BeltActor.States = BeltActor.States.LOADING

  private val daprClient = DaprClientBuilder().build()

  private fun transition(targetState: BeltActor.States, data: Any? = null) {
    when (targetState) {
      BeltActor.States.LOADING -> {
        if (currentActiveState == BeltActor.States.UNLOADING) {
          // Exit actions
          unregisterTimer("armPickupTimeout-${id}").subscribe()

          currentActiveState = targetState
        }
      }

      BeltActor.States.TRANSPORTING -> {
        if (currentActiveState == BeltActor.States.LOADING) {
          currentActiveState = BeltActor.States.TRANSPORTING
          transportingState()
        }
      }

      BeltActor.States.UNLOADING -> {
        if (currentActiveState == BeltActor.States.TRANSPORTING) {
          // Exit actions
          Services.stopBelt().subscribe()

          currentActiveState = BeltActor.States.UNLOADING
          unloadingState()
        }
      }

      BeltActor.States.JOB_DONE -> {
        // Exit actions
        if (currentActiveState == BeltActor.States.UNLOADING)
          unregisterTimer("armPickupTimeout-${id}").subscribe()

        currentActiveState = BeltActor.States.JOB_DONE
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
    transition(BeltActor.States.TRANSPORTING)
  }

  override fun startUnloading() {
    transition(BeltActor.States.UNLOADING)
  }

  override fun markJobDone() {
    transition(BeltActor.States.JOB_DONE)
  }

  override fun markPickedUp() {
    transition(BeltActor.States.LOADING)
  }

  override fun armPickupTimeout(): Mono<Void> {
    if (currentActiveState == BeltActor.States.UNLOADING) {
      // Raising eArmPickup
      Utils.publishEvent(daprClient, "pubsub", "eArmPickup", mutableMapOf<String, Any?>())
        .subscribe()
    }

    return Mono.empty()
  }
}
