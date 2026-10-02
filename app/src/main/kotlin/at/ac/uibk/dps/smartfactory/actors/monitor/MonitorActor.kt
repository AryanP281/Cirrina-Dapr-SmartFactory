package at.ac.uibk.dps.smartfactory.actors.monitor

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "Monitor")
interface MonitorActor {

  @ActorMethod(name = "markScanned") fun markScanned()

  @ActorMethod(name = "markAssembled") fun markAssembled()

  @ActorMethod(name = "markJobDone") fun markJobDone()
}
