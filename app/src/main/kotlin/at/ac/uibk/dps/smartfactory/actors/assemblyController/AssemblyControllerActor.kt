package at.ac.uibk.dps.smartfactory.actors.assemblyController

import io.dapr.actors.ActorType

@ActorType(name = "AssemblyControllerActor")
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
