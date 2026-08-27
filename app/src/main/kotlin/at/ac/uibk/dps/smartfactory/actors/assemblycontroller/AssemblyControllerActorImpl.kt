package at.ac.uibk.dps.smartfactory.actors.assemblycontroller

import at.ac.uibk.dps.smartfactory.api.PhotoScanRequest
import at.ac.uibk.dps.smartfactory.logger
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
  private var waitingParts: Long = 0

  private var currentActiveState: AssemblyControllerActor.States =
    AssemblyControllerActor.States.DETECTING_START

  private val daprClient = DaprClientBuilder().build()

  // Measurement vars
  var firstDetection = true

  private fun transition(targetState: AssemblyControllerActor.States, data: Any? = null) {
    when (targetState) {
      AssemblyControllerActor.States.CAPTURE_PHOTO -> {
        if (currentActiveState == AssemblyControllerActor.States.DETECTING_START) {
          currentActiveState = AssemblyControllerActor.States.CAPTURE_PHOTO
          capturePhotoState()
        }
      }

      AssemblyControllerActor.States.SCAN_PHOTO -> {
        if (currentActiveState == AssemblyControllerActor.States.CAPTURE_PHOTO) {
          currentActiveState = AssemblyControllerActor.States.SCAN_PHOTO
          scanPhotoState(data as ByteArray)
        }
      }

      AssemblyControllerActor.States.DETECTING_END -> {
        if (currentActiveState == AssemblyControllerActor.States.SCAN_PHOTO)
          currentActiveState = AssemblyControllerActor.States.DETECTING_END
      }

      AssemblyControllerActor.States.ERROR -> {
        if (currentActiveState == AssemblyControllerActor.States.SCAN_PHOTO) {
          currentActiveState = AssemblyControllerActor.States.ERROR
          errorState()
        }
      }

      AssemblyControllerActor.States.DETECTING_START -> {
        if (
          currentActiveState == AssemblyControllerActor.States.ERROR ||
            currentActiveState == AssemblyControllerActor.States.UNLOADING
        )
          currentActiveState = AssemblyControllerActor.States.DETECTING_START
        detectingStartState()
      }

      AssemblyControllerActor.States.UNLOADING -> {
        if (currentActiveState == AssemblyControllerActor.States.DETECTING_END)
          currentActiveState = AssemblyControllerActor.States.UNLOADING
      }

      AssemblyControllerActor.States.JOB_DONE -> {
        currentActiveState = AssemblyControllerActor.States.JOB_DONE
      }
    }

    logger.info("In state: ${currentActiveState.name}")
  }

  private fun detectingStartState() {
    if (waitingParts > 0) {
      waitingParts--
      transition(AssemblyControllerActor.States.CAPTURE_PHOTO)
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
    Utils.publishEvent(daprClient,
      "pubsub",
      "eProcessMessage",
      mutableMapOf("msg" to "Assembly error: Invalid object detected")).subscribe()

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
      Utils.publishEvent(daprClient,"pubsub", "eProductionStarted", mutableMapOf()).subscribe()
      firstDetection = false
    }

    transition(AssemblyControllerActor.States.CAPTURE_PHOTO)
  }

  override fun processCapturedPhoto(photoData: ByteArray) {
    transition(AssemblyControllerActor.States.SCAN_PHOTO, photoData)
  }

  override fun processPhotoScan(scanStatus: Boolean) {
    // Raising eScanned
    Utils.publishEvent(daprClient,"pubsub", "eScanned", mutableMapOf<String, Any?>()).subscribe()

    if (scanStatus) {
      // Raise eObjectValid
      Utils.publishEvent(daprClient,"pubsub", "eObjectValid", mutableMapOf<String, Any?>()).subscribe()

      transition(AssemblyControllerActor.States.DETECTING_END)
    } else transition(AssemblyControllerActor.States.ERROR)
  }

  override fun objectDiscarded() {
    transition(AssemblyControllerActor.States.DETECTING_START)
  }

  override fun detectedAtEnd() {
    // Raising eStartUnload
    Utils.publishEvent(daprClient,"pubsub", "eStartUnload", mutableMapOf<String, Any?>()).subscribe()

    transition(AssemblyControllerActor.States.UNLOADING)
  }

  override fun processPickup() {
    if (currentActiveState == AssemblyControllerActor.States.UNLOADING)
      transition(AssemblyControllerActor.States.DETECTING_START)
  }

  override fun markJobDone() {
    transition(AssemblyControllerActor.States.JOB_DONE)
  }
}
