package at.ac.uibk.dps.smartfactory.actors.arm

import at.ac.uibk.dps.smartfactory.services.Services
import at.ac.uibk.dps.smartfactory.utils.Utils
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.time.Duration
import reactor.core.publisher.Mono

class ArmActorImpl(runtimeContext: ActorRuntimeContext<ArmActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), ArmActor {

  enum class State {
    IDLE,
    ERROR,
    PICKUP,
    ASSEMBLE,
    RETURN,
    JOB_DONE,
  }

  private val partsPerProduct = 1000
  private var currActiveState = State.IDLE
  private var pickupSuccess = true
  private var errorMsg = ""
  private var partsAssembled = 0

  private val daprClient = DaprClientBuilder().build()

  private fun transition(targetState: State, data: Any? = null) {
    when (targetState) {
      State.IDLE -> {
        if (currActiveState == State.RETURN) {
          // Transition Actions
          if (partsAssembled >= partsPerProduct) {
            partsAssembled = 0
            Utils.publishEvent(
                daprClient,
                "pubsub",
                "eProductComplete",
                mutableMapOf<String, Any?>(),
              )
              .subscribe()
          }

          currActiveState = targetState
          idleState()
        }
      }

      State.PICKUP -> {
        if (currActiveState == State.IDLE) {
          currActiveState = targetState
          pickupState()
        }
      }

      State.ASSEMBLE -> {
        if (currActiveState == State.PICKUP || currActiveState == State.ERROR) {
          // Transition actions
          if (currActiveState == State.PICKUP) {
            // Raise ePickedUp
            Utils.publishEvent(daprClient, "pubsub", "ePickedUp", mutableMapOf<String, Any?>())
              .subscribe()
          }

          currActiveState = targetState
          assembleState()
        }
      }

      State.ERROR -> {
        if (
          currActiveState == State.PICKUP || currActiveState == State.ASSEMBLE
        ) {
          // Transition actions
          if (currActiveState == State.PICKUP) errorMsg = "Pickup failed..."
          if (currActiveState == State.ASSEMBLE) errorMsg = "Assemble failed..."

          currActiveState = targetState
          errorState()
        }
      }

      State.RETURN -> {
        if (
          currActiveState == State.ASSEMBLE || currActiveState == State.ERROR
        ) {
          // Transition Actions
          if (currActiveState == State.ASSEMBLE) {
            partsAssembled++
            Utils.publishEvent(
                daprClient,
                "pubsub",
                "eAssemblyComplete",
                mutableMapOf<String, Any?>(),
              )
              .subscribe()
          }

          currActiveState = targetState
          returnState()
        }
      }

      State.JOB_DONE -> {
        currActiveState = targetState
      }
    }
  }

  override fun initialize() {
    idleState()
  }

  override fun initiatePickup() {
    val isJobDone =
      daprClient.getState("statestore", "isJobDone", Boolean::class.java).block()?.value ?: false

    if (!isJobDone) transition(State.PICKUP)
  }

  override fun updatePickupStatus(pickupStatus: Boolean) {
    this.pickupSuccess = pickupStatus

    if (pickupStatus) transition(State.ASSEMBLE) else transition(State.ERROR)
  }

  override fun updateAssemblyStatus(assemblyStatus: Boolean) {
    if (assemblyStatus) transition(State.RETURN) else transition(State.ERROR)
  }

  override fun markJobDone() {
    if (currActiveState == State.IDLE) transition(State.JOB_DONE)
  }

  override fun markPickedUp() {
    this.pickupSuccess = true
  }

  override fun armReset() {
    transition(State.IDLE)
  }

  private fun idleState() {
    if (!pickupSuccess) transition(State.PICKUP)
  }

  private fun pickupState() {
    // Invoke arm pickup
    Services.pickUp().subscribe()
  }

  private fun assembleState() {
    // Invoke Assemble
    Services.assemble().subscribe()
  }

  private fun errorState() {
    // Raise eProcessMessage
    Utils.publishEvent(
        daprClient,
        "pubsub",
        "eProcessMessage",
        mutableMapOf("msg" to "Fatal robotic arm failure: $errorMsg"),
      )
      .subscribe()

    // Starting timer eRetry
    registerActorTimer(
        "eRetryTimer",
        "retryTimeout",
        0,
        Duration.ofSeconds(2),
        Duration.ofMillis(-1),
      )
      .subscribe()
  }

  private fun returnState() {
    // Invoke return to start action
    Services.returnToStart().subscribe()
  }

  override fun retryTimeout(): Mono<Void> {
    if (currActiveState == State.ERROR) {
      if (pickupSuccess) transition(State.ASSEMBLE)
      else transition(State.RETURN)
    }

    return Mono.empty()
  }
}
