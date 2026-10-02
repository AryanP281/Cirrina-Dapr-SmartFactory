package at.ac.uibk.dps.smartfactory.actors.assemblycontroller

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "assemblyController")
interface AssemblyControllerActor {

  @ActorMethod(name = "initialize") fun initialize()

  @ActorMethod(name = "detectedAtStart") fun detectedAtStart()

  @ActorMethod(name = "processCapturedPhoto") fun processCapturedPhoto(photoData: ByteArray)

  @ActorMethod(name = "processPhotoScan") fun processPhotoScan(scanStatus: Boolean)

  @ActorMethod(name = "objectDiscarded") fun objectDiscarded()

  @ActorMethod(name = "detectedAtEnd") fun detectedAtEnd()

  @ActorMethod(name = "processPickup") fun processPickup()

  @ActorMethod(name = "markJobDone") fun markJobDone()
}
