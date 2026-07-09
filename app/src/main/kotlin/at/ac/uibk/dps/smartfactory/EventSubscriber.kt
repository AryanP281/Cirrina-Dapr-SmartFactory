package at.ac.uibk.dps.smartfactory

import at.ac.uibk.dps.smartfactory.actors.arm.ArmActor
import at.ac.uibk.dps.smartfactory.actors.assemblycontroller.AssemblyControllerActor
import at.ac.uibk.dps.smartfactory.actors.belt.BeltActor
import at.ac.uibk.dps.smartfactory.actors.jobcontroller.JobControllerActor
import at.ac.uibk.dps.smartfactory.actors.messageprocessor.MessageProcessorActor
import at.ac.uibk.dps.smartfactory.actors.monitor.MonitorActor
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.context.annotation.Profile
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@Profile("!test")
@RestController
class EventSubscriber {
    private val role = System.getenv("ROLE")
    private val actorId = System.getenv("ACTOR_ID") ?: "actor-0"

    val actorProxy = when(role) {
        "jobcontroller" -> ActorProxyBuilder(JobControllerActor::class.java, ActorClient()).build(ActorId(actorId))
        "messageprocessor" -> ActorProxyBuilder(MessageProcessorActor::class.java, ActorClient()).build(ActorId(actorId))
        "assemblycontroller" -> ActorProxyBuilder(AssemblyControllerActor::class.java, ActorClient()).build(ActorId(actorId))
        "monitor" -> ActorProxyBuilder(MonitorActor::class.java, ActorClient()).build(ActorId(actorId))
        "belt" -> ActorProxyBuilder(BeltActor::class.java, ActorClient()).build(ActorId(actorId))
        else -> ActorProxyBuilder(ArmActor::class.java, ActorClient()).build(ActorId(actorId))
    }

    @Topic(name = "eBeamInterruptedStart", pubsubName = "pubsub")
    @PostMapping("/eBeamInterruptedStart")
    fun eBeamInterruptedStart() : ResponseEntity<Unit> {
        when(actorProxy) {
            is AssemblyControllerActor -> (actorProxy as AssemblyControllerActor).detectedAtStart()
        }

        return ResponseEntity.ok().build()
    }

    @Topic(name = "ePhotoCaptured", pubsubName = "pubsub")
    @PostMapping("/ePhotoCaptured")
    fun ePhotoCaptured(@RequestBody event : CloudEvent<Map<String, ByteArray>>) : ResponseEntity<Unit> {
        when(actorProxy) {
            is AssemblyControllerActor -> (actorProxy as AssemblyControllerActor).processCapturedPhoto(event.data["data"] ?: byteArrayOf())
        }

        return ResponseEntity.ok().build()
    }

    @Topic(name = "ePhotoScanned", pubsubName = "pubsub")
    @PostMapping("/ePhotoScanned")
    fun ePhotoScanned(@RequestBody event : CloudEvent<Map<String, Boolean>>) : ResponseEntity<Unit> {
        when(actorProxy) {
            is AssemblyControllerActor -> (actorProxy as AssemblyControllerActor).processPhotoScan(event.data["validObject"] ?: false)
        }

        return ResponseEntity.ok().build()
    }

    @Topic(name = "eObjectDiscarded", pubsubName = "pubsub")
    @PostMapping("/eObjectDiscarded")
    fun eObjectDiscarded() : ResponseEntity<Unit> {
        when(actorProxy) {
            is AssemblyControllerActor -> (actorProxy as AssemblyControllerActor).objectDiscarded()
        }

        return ResponseEntity.ok().build()
    }

    @Topic(name = "eBeamInterruptedEnd", pubsubName = "pubsub")
    @PostMapping("/eBeamInterruptedEnd")
    fun eBeamInterruptedEnd() : ResponseEntity<Unit> {
        when(actorProxy) {
            is AssemblyControllerActor -> (actorProxy as AssemblyControllerActor).detectedAtEnd()
        }

        return ResponseEntity.ok().build()
    }

    @Topic(name = "eProductComplete", pubsubName = "pubsub")
    @PostMapping("/eProductComplete")
    fun newProductComplete() : ResponseEntity<Unit> {
        when(actorProxy){
            is JobControllerActor -> (actorProxy as JobControllerActor).markProductCompleted()
            is MonitorActor -> (actorProxy as MonitorActor).incrementProductsCompletedCount()
        }

        return ResponseEntity.ok().build()
    }

    @Topic(name = "eProcessMessage", pubsubName = "pubsub")
    @PostMapping("/eProcessMessage")
    fun processMessage(@RequestBody event: CloudEvent<Map<String, String>>) : ResponseEntity<Unit>
    {
        if(actorProxy is MessageProcessorActor)
            (actorProxy as MessageProcessorActor).processMessage(event.data["msg"] ?: "")
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eScanned", pubsubName = "pubsub")
    @PostMapping("/eScanned")
    fun incrementScannedCount() : ResponseEntity<Unit> {
        when(actorProxy) {
            is MonitorActor -> (actorProxy as MonitorActor).markScanned()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eAssemblyComplete", pubsubName = "pubsub")
    @PostMapping("/eAssemblyComplete")
    fun incrementAssembledCount() : ResponseEntity<Unit> {
        when(actorProxy) {
            is MonitorActor -> (actorProxy as MonitorActor).markAssembled()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eObjectValid", pubsubName = "pubsub")
    @PostMapping("/eObjectValid")
    fun setObjectValidity() : ResponseEntity<Unit> {
        when(actorProxy) {
            is BeltActor -> (actorProxy as BeltActor).markObjectValidity()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eStartUnload", pubsubName = "pubsub")
    @PostMapping("/eStartUnload")
    fun startUnloading() : ResponseEntity<Unit> {
        when(actorProxy) {
            is BeltActor -> (actorProxy as BeltActor).startUnloading()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "ePickedUp", pubsubName = "pubsub")
    @PostMapping("/ePickedUp")
    fun markPickedUp() : ResponseEntity<Unit> {
        when(actorProxy) {
            is BeltActor -> (actorProxy as BeltActor).markPickedUp()
            is AssemblyControllerActor -> actorProxy.processPickup()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eArmPickup", pubsubName = "pubsub")
    @PostMapping("/eArmPickup")
    fun pickup() : ResponseEntity<Unit> {
        when(actorProxy) {
            is ArmActor -> actorProxy.initiatePickup()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eMarkPickedUp", pubsubName = "pubsub")
    @PostMapping("/eMarkPickedUp")
    fun eMarkPickedUp() : ResponseEntity<Unit> {
        when(actorProxy) {
            is ArmActor -> (actorProxy as ArmActor).markPickedUp()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eUpdatePickupStatus", pubsubName = "pubsub")
    @PostMapping("/eUpdatePickupStatus")
    fun eUpdatePickupStatus(@RequestBody event: CloudEvent<Boolean>) : ResponseEntity<Unit> {
        when(actorProxy) {
            is ArmActor -> actorProxy.updatePickupStatus(event.data)
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eCheckAssembleSuccess", pubsubName = "pubsub")
    @PostMapping("/eCheckAssembleSuccess")
    fun eCheckAssembleSuccess(@RequestBody event: CloudEvent<Boolean>) : ResponseEntity<Unit> {
        when(actorProxy) {
            is ArmActor -> (actorProxy as ArmActor).updateAssemblyStatus(event.data)
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eResetArm", pubsubName = "pubsub")
    @PostMapping("/eResetArm")
    fun eResetArm() : ResponseEntity<Unit> {
        when(actorProxy) {
            is ArmActor -> actorProxy.armReset()
        }
        return ResponseEntity.ok().build()
    }

    @Topic(name = "eJobDone", pubsubName = "pubsub")
    @PostMapping("/eJobDone")
    fun markJobDone() : ResponseEntity<Unit> {
        when(actorProxy) {
            is MessageProcessorActor -> (actorProxy as MessageProcessorActor).markJobDone()
            is MonitorActor -> (actorProxy as MonitorActor).markJobDone()
            is BeltActor -> (actorProxy as BeltActor).markJobDone()
            is ArmActor -> (actorProxy as ArmActor).markJobDone()
        }
        return ResponseEntity.ok().build()
    }
}
