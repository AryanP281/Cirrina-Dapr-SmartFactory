package at.ac.uibk.dps.smartfactory.actors.assemblycontroller

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "assemblyController")
interface AssemblyControllerActor {

  fun initialize()

  fun detectedAtStart()

   fun processCapturedPhoto(photoData: ByteArray)

   fun processPhotoScan(scanStatus: Boolean)

   fun objectDiscarded()

   fun detectedAtEnd()

   fun processPickup()

   fun markJobDone()
}
