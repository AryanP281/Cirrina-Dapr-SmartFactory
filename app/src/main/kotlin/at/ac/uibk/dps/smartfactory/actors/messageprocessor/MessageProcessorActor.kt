package at.ac.uibk.dps.smartfactory.actors.messageprocessor

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "MessageProcessor")
interface MessageProcessorActor {

  @ActorMethod(name = "processMessage") fun processMessage(message: String)

  @ActorMethod(name = "markJobDone") fun markJobDone()
}
