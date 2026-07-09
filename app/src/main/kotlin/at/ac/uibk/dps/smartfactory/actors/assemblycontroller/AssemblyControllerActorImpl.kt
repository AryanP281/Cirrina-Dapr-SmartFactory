package at.ac.uibk.dps.smartfactory.actors.assemblycontroller

import at.ac.uibk.dps.smartfactory.services.PhotoScanRequest
import at.ac.uibk.dps.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class AssemblyControllerActorImpl(
    runtimeContext : ActorRuntimeContext<AssemblyControllerActorImpl>,
    id: ActorId
) : AbstractActor(runtimeContext, id), AssemblyControllerActor {

    private var currentActiveState : AssemblyControllerActor.States = AssemblyControllerActor.States.DETECTING_START

    private val daprClient = DaprClientBuilder().build()

    private fun transition(targetState: AssemblyControllerActor.States, data: Any? = null) {
        when(targetState) {
            AssemblyControllerActor.States.CAPTURE_PHOTO -> {
                if(currentActiveState == AssemblyControllerActor.States.DETECTING_START)
                {
                    currentActiveState = AssemblyControllerActor.States.CAPTURE_PHOTO
                    capturePhotoState()
                }
            }

            AssemblyControllerActor.States.SCAN_PHOTO -> {
                if(currentActiveState == AssemblyControllerActor.States.CAPTURE_PHOTO)
                {
                    currentActiveState = AssemblyControllerActor.States.SCAN_PHOTO
                    scanPhotoState(data as ByteArray)
                }
            }

            AssemblyControllerActor.States.DETECTING_END -> {
                if(currentActiveState == AssemblyControllerActor.States.SCAN_PHOTO)
                    currentActiveState = AssemblyControllerActor.States.DETECTING_END
            }

            AssemblyControllerActor.States.ERROR -> {
                if(currentActiveState == AssemblyControllerActor.States.SCAN_PHOTO)
                {
                    currentActiveState = AssemblyControllerActor.States.ERROR
                    errorState()
                }
            }

            AssemblyControllerActor.States.DETECTING_START -> {
                if(currentActiveState == AssemblyControllerActor.States.ERROR || currentActiveState == AssemblyControllerActor.States.UNLOADING)
                    currentActiveState = AssemblyControllerActor.States.DETECTING_START
            }

            AssemblyControllerActor.States.UNLOADING -> {
                if(currentActiveState == AssemblyControllerActor.States.DETECTING_END)
                    currentActiveState = AssemblyControllerActor.States.UNLOADING
            }

            AssemblyControllerActor.States.JOB_DONE -> {
                currentActiveState = AssemblyControllerActor.States.JOB_DONE
            }
        }
    }

    private fun capturePhotoState()
    {
        //Invoking photo capture service
        Services.takePhoto()
    }

    private fun scanPhotoState(photoData : ByteArray)
    {
        //Invoking photo scan service
        Services.scanPhoto(PhotoScanRequest(photoData))
    }

    private fun errorState()
    {
        //Raise eProcessMessage
        daprClient.publishEvent("pubsub", "eProcessMessage", mapOf("msg" to "Belt error: Invalid object detected")).subscribe()

        Services.discardObject().subscribe()
    }

    override fun detectedAtStart()
    {
        transition(AssemblyControllerActor.States.CAPTURE_PHOTO)
    }

    override fun processCapturedPhoto(photoData : ByteArray) {
        transition(AssemblyControllerActor.States.SCAN_PHOTO, photoData)
    }

    override fun processPhotoScan(scanStatus: Boolean) {
        //Raising eScanned
        daprClient.publishEvent("pubsub", "eScanned", mapOf<String,Any>()).subscribe()

        if(scanStatus)
        {
            //Raise eObjectValid
            daprClient.publishEvent("pubsub", "eObjectValid", mapOf<String,Any>()).subscribe()

            transition(AssemblyControllerActor.States.DETECTING_END)
        }
        else
            transition(AssemblyControllerActor.States.ERROR)
    }

    override fun objectDiscarded() {
        transition(AssemblyControllerActor.States.DETECTING_START)
    }

    override fun detectedAtEnd() {
        //Raising eStartUnload
        daprClient.publishEvent("pubsub", "eStartUnload",mapOf<String,Any>()).subscribe()

        transition(AssemblyControllerActor.States.UNLOADING)
    }

    override fun processPickup() {
        if(currentActiveState == AssemblyControllerActor.States.UNLOADING)
            transition(AssemblyControllerActor.States.DETECTING_START)
    }

    override fun markJobDone() {
        transition(AssemblyControllerActor.States.JOB_DONE)
    }

}