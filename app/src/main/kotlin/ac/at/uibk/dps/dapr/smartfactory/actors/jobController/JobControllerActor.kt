package ac.at.uibk.dps.dapr.smartfactory.actors.jobController

import io.dapr.actors.ActorType

@ActorType(name = "JobControllerActor")
interface JobControllerActor {
  fun initialize()

  fun markProductCompleted()
}
