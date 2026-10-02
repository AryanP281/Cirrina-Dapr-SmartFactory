package at.ac.uibk.dps.smartfactory.actors.monitor

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "MonitorActor")
interface MonitorActor {

   fun markScanned()

   fun markAssembled()

   fun markJobDone()
}
