package at.ac.uibk.dps.smartfactory.actors.monitor

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "Monitor")
interface MonitorActor {

   fun markScanned()

   fun markAssembled()

   fun markJobDone()
}
