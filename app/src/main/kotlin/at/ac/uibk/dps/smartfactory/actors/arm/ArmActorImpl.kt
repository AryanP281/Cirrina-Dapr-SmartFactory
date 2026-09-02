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
  private val partsPerProduct = 1000
  private var currActiveState = ArmActor.States.IDLE
  private var pickupSuccess = true
  private var errorMsg = ""
  private var partsAssembled = 0

  private val daprClient = DaprClientBuilder().build()

  private fun transition(targetState: ArmActor.States, data: Any? = null) {
    when (targetState) {
      ArmActor.States.IDLE -> {
        if (currActiveState == ArmActor.States.RETURN) {
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

      ArmActor.States.PICKUP -> {
        if (currActiveState == ArmActor.States.IDLE) {
          currActiveState = targetState
          pickupState()
        }
      }

      ArmActor.States.ASSEMBLE -> {
        if (currActiveState == ArmActor.States.PICKUP || currActiveState == ArmActor.States.ERROR) {
          // Transition actions
          if (currActiveState == ArmActor.States.PICKUP) {
            // Raise ePickedUp
            Utils.publishEvent(daprClient, "pubsub", "ePickedUp", mutableMapOf<String, Any?>())
              .subscribe()
          }

          currActiveState = targetState
          assembleState()
        }
      }

      ArmActor.States.ERROR -> {
        if (
          currActiveState == ArmActor.States.PICKUP || currActiveState == ArmActor.States.ASSEMBLE
        ) {
          // Transition actions
          if (currActiveState == ArmActor.States.PICKUP) errorMsg = "Pickup failed..."
          if (currActiveState == ArmActor.States.ASSEMBLE) errorMsg = "Assemble failed..."

          currActiveState = targetState
          errorState()
        }
      }

      ArmActor.States.RETURN -> {
        if (
          currActiveState == ArmActor.States.ASSEMBLE || currActiveState == ArmActor.States.ERROR
        ) {
          // Transition Actions
          if (currActiveState == ArmActor.States.ASSEMBLE) {
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

      ArmActor.States.JOB_DONE -> {
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

    if (!isJobDone) transition(ArmActor.States.PICKUP)
  }

  override fun updatePickupStatus(pickupStatus: Boolean) {
    this.pickupSuccess = pickupStatus

    if (pickupStatus) transition(ArmActor.States.ASSEMBLE) else transition(ArmActor.States.ERROR)
  }

  override fun updateAssemblyStatus(assemblyStatus: Boolean) {
    if (assemblyStatus) transition(ArmActor.States.RETURN) else transition(ArmActor.States.ERROR)
  }

  override fun markJobDone() {
    if (currActiveState == ArmActor.States.IDLE) transition(ArmActor.States.JOB_DONE)
  }

  override fun markPickedUp() {
    this.pickupSuccess = true
  }

  override fun armReset() {
    transition(ArmActor.States.IDLE)
  }

  private fun idleState() {
    if (!pickupSuccess) transition(ArmActor.States.PICKUP)
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
    if (currActiveState == ArmActor.States.ERROR) {
      if (pickupSuccess) transition(ArmActor.States.ASSEMBLE)
      else transition(ArmActor.States.RETURN)
    }

    return Mono.empty()
  }
}
