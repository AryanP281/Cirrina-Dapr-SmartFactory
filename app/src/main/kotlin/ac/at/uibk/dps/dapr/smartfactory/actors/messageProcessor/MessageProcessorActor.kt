package ac.at.uibk.dps.dapr.smartfactory.actors.messageProcessor

import io.dapr.actors.ActorType

@ActorType(name = "MessageProcessorActor")
interface MessageProcessorActor {

  fun processMessage(message: String)

  fun markJobDone()
}
