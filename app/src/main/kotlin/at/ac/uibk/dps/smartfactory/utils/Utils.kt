package at.ac.uibk.dps.smartfactory.utils

import kotlin.time.Clock

object Utils
{
    fun getEmittedTimeNs() : Long
    {
        val now = Clock.System.now()

        return now.epochSeconds*1_000_000_000L + now.nanosecondsOfSecond
    }

}