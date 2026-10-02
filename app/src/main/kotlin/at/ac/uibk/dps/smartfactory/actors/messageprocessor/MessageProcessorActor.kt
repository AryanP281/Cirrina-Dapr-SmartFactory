package at.ac.uibk.dps.smartfactory.actors.messageprocessor

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "MessageProcessor")
interface MessageProcessorActor {

   fun processMessage(message: String)

   fun markJobDone()
}
