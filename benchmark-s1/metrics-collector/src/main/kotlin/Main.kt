package org.example

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication class DaprSmartFactoryMetricsCollector

val logger = LoggerFactory.getLogger("org.example.MainKt")

fun main(args: Array<String>)
{
    logger.info("Metrics collector started")

    runApplication<DaprSmartFactoryMetricsCollector>(*args)
}