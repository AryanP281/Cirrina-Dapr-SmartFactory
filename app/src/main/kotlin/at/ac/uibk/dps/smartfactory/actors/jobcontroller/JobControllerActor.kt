package at.ac.uibk.dps.smartfactory.actors.jobcontroller

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "JobController")
interface JobControllerActor {
  @ActorMethod(name = "initialize") fun initialize()

  @ActorMethod(name = "markProductCompleted") fun markProductCompleted()
}
