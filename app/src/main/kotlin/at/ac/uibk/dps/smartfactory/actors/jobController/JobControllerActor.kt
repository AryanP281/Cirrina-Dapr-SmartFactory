package at.ac.uibk.dps.smartfactory.actors.jobcontroller

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "JobControllerActor")
interface JobControllerActor {
   fun initialize()

   fun markProductCompleted()
}
