package at.ac.uibk.dps.smartfactory.actors.belt

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "Belt")
interface BeltActor {

   fun markObjectValidity()

   fun startUnloading()

   fun markJobDone()

   fun markPickedUp()

   fun armPickupTimeout(): Mono<Void>
}
