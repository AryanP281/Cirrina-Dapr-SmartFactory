package at.ac.uibk.dps.smartfactory.actors.assemblycontroller

import at.ac.uibk.dps.smartfactory.api.PhotoScanRequest
import at.ac.uibk.dps.smartfactory.services.Services
import at.ac.uibk.dps.smartfactory.utils.Utils
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class AssemblyControllerActorImpl(
  runtimeContext: ActorRuntimeContext<AssemblyControllerActorImpl>,
  id: ActorId,
) : AbstractActor(runtimeContext, id), AssemblyControllerActor {

  enum class State {
    DETECTING_START,
    CAPTURE_PHOTO,
    SCAN_PHOTO,
    ERROR,
    DETECTING_END,
    UNLOADING,
    JOB_DONE,
  }

  private var waitingParts: Long = 0

  private var currentActiveState: State =
    State.DETECTING_START

  private val daprClient = DaprClientBuilder().build()

  // Measurement vars
  var firstDetection = true

  private fun transition(targetState: State, data: Any? = null) {
    when (targetState) {
      State.CAPTURE_PHOTO -> {
        if (currentActiveState == State.DETECTING_START) {
          waitingParts -= 1
          currentActiveState = State.CAPTURE_PHOTO
          capturePhotoState()
        }
      }

      State.SCAN_PHOTO -> {
        if (currentActiveState == State.CAPTURE_PHOTO) {
          currentActiveState = State.SCAN_PHOTO
          scanPhotoState(data as ByteArray)
        }
      }

      State.DETECTING_END -> {
        if (currentActiveState == State.SCAN_PHOTO)
          currentActiveState = State.DETECTING_END
      }

      State.ERROR -> {
        if (currentActiveState == State.SCAN_PHOTO) {
          currentActiveState = State.ERROR
          errorState()
        }
      }

      State.DETECTING_START -> {
        if (
          currentActiveState == State.ERROR ||
            currentActiveState == State.UNLOADING
        ) {
          currentActiveState = State.DETECTING_START
          detectingStartState()
        }
      }

      State.UNLOADING -> {
        if (currentActiveState == State.DETECTING_END)
          currentActiveState = State.UNLOADING
      }

      State.JOB_DONE -> {
        currentActiveState = State.JOB_DONE
      }
    }
  }

  private fun detectingStartState() {
    if (waitingParts > 0) {
      transition(State.CAPTURE_PHOTO)
    }
  }

  private fun capturePhotoState() {
    // Invoking photo capture service
    Services.takePhoto().subscribe()
  }

  private fun scanPhotoState(photoData: ByteArray) {
    // Invoking photo scan service
    Services.scanPhoto(PhotoScanRequest(photoData)).subscribe()
  }

  private fun errorState() {
    // Raise eProcessMessage
    Utils.publishEvent(
        daprClient,
        "pubsub",
        "eProcessMessage",
        mutableMapOf("msg" to "Assembly error: Invalid object detected"),
      )
      .subscribe()

    Services.discardObject().subscribe()
  }

  override fun initialize() {
    detectingStartState()
  }

  override fun detectedAtStart() {
    waitingParts = if (waitingParts == Long.MAX_VALUE) Long.MAX_VALUE else waitingParts + 1

    // Checking if first detection
    if (firstDetection) {
      // Emitting event to begin production time measurement
      Utils.publishEvent(daprClient, "pubsub", "eProductionStarted", mutableMapOf()).subscribe()
      firstDetection = false
    }

    transition(State.CAPTURE_PHOTO)
  }

  override fun processCapturedPhoto(photoData: ByteArray) {
    transition(State.SCAN_PHOTO, photoData)
  }

  override fun processPhotoScan(scanStatus: Boolean) {
    // Raising eScanned
    Utils.publishEvent(daprClient, "pubsub", "eScanned", mutableMapOf<String, Any?>()).subscribe()

    if (scanStatus) {
      // Raise eObjectValid
      Utils.publishEvent(daprClient, "pubsub", "eObjectValid", mutableMapOf<String, Any?>())
        .subscribe()

      transition(State.DETECTING_END)
    } else transition(State.ERROR)
  }

  override fun objectDiscarded() {
    transition(State.DETECTING_START)
  }

  override fun detectedAtEnd() {
    // Raising eStartUnload
    Utils.publishEvent(daprClient, "pubsub", "eStartUnload", mutableMapOf<String, Any?>())
      .subscribe()

    transition(State.UNLOADING)
  }

  override fun processPickup() {
    if (currentActiveState == State.UNLOADING)
      transition(State.DETECTING_START)
  }

  override fun markJobDone() {
    transition(State.JOB_DONE)
  }
}
