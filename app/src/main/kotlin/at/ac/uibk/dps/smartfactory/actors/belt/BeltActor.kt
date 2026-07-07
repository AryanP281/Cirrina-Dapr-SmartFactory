package at.ac.uibk.dps.smartfactory.actors.belt

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "Belt")
interface BeltActor {

    enum class States{
        LOADING,
        TRANSPORTING,
        UNLOADING,
        ERROR,
        JOB_DONE
    }

    @ActorMethod(name = "markObjectValidity")
    fun markObjectValidity(isValid: Boolean)

    @ActorMethod(name="startUnloading")
    fun startUnloading()

    @ActorMethod(name="markJobDone")
    fun markJobDone()

    @ActorMethod(name="markPickedUp")
    fun markPickedUp()

    @ActorMethod(name="armPickupTimeout")
    fun armPickupTimeout() : Mono<Void>
}

