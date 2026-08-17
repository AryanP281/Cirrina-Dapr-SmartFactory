package at.ac.uibk.dps.smartfactory.actors.messageprocessor

import at.ac.uibk.dps.smartfactory.api.MessageProcessingRequest
import at.ac.uibk.dps.smartfactory.services.Services
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class MessageProcessorImpl(
  runtimeContext: ActorRuntimeContext<MessageProcessorImpl>,
  actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), MessageProcessorActor {
  private var currentActiveState: MessageProcessorActor.States = MessageProcessorActor.States.IDLE
  private val processorType: MessageProcessorActor.ProcessorType =
    (System.getenv("MP_TYPE") ?: "0").let {
      when (it) {
        "0" -> MessageProcessorActor.ProcessorType.EMAIL
        "1" -> MessageProcessorActor.ProcessorType.SMS
        else -> MessageProcessorActor.ProcessorType.LOG
      }
    }

  private val daprClient = DaprClientBuilder().build()

  private fun transition(targetState: MessageProcessorActor.States, data: Any? = null) {
    if (currentActiveState == MessageProcessorActor.States.JOB_DONE) return

    when (targetState) {
      MessageProcessorActor.States.IDLE -> {
        currentActiveState = MessageProcessorActor.States.IDLE
      }
      MessageProcessorActor.States.PROCESS -> {
        if (currentActiveState == MessageProcessorActor.States.IDLE) {
          currentActiveState = MessageProcessorActor.States.PROCESS
          processState(data as String)
        }
      }
      MessageProcessorActor.States.JOB_DONE -> {
        if (currentActiveState == MessageProcessorActor.States.IDLE)
          currentActiveState = MessageProcessorActor.States.JOB_DONE
      }
    }
  }

  override fun processMessage(message: String) {
    if (currentActiveState == MessageProcessorActor.States.IDLE)
      transition(MessageProcessorActor.States.PROCESS, message)
  }

  override fun markJobDone() {
    if (currentActiveState == MessageProcessorActor.States.IDLE)
      transition(MessageProcessorActor.States.JOB_DONE)
  }

  private fun processState(msg: String) {
    handleMessage(msg)

    transition(MessageProcessorActor.States.IDLE)
  }

  private fun handleMessage(msg: String) {
    when (processorType) {
      MessageProcessorActor.ProcessorType.EMAIL -> {
        Services.processEmail(MessageProcessingRequest(msg)).subscribe()
      }
      MessageProcessorActor.ProcessorType.SMS -> {
        Services.processSms(MessageProcessingRequest(msg)).subscribe()
      }
      MessageProcessorActor.ProcessorType.LOG -> {
        val logs: MutableList<String> =
          daprClient.getState("statestore", "logs", MutableList::class.java).block()?.value
            as MutableList<String>? ?: mutableListOf<String>()
        logs.add(msg)
        daprClient.saveState("statestore", "logs", logs).block()
      }
    }
  }
}
