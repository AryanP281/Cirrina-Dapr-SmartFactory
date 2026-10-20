package at.ac.uibk.dps.dapr.execution.`object`

import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.io.File
import java.util.concurrent.TimeUnit

@SpringBootApplication class DaprSmartFactoryMetricsCollector

val metricsPeriod = System.getenv("METRICS_PERIOD")?.toLong() ?: 1L
val metricsRegistry = MetricRegistry()

val logger = LoggerFactory.getLogger("at.ac.uibk.dps.dapr.execution.object.MetricsCollectorKt")

fun main(args: Array<String>) {
  CsvReporter.forRegistry(metricsRegistry).build(File("./metrics")).start(metricsPeriod, TimeUnit.SECONDS)

  logger.info("Metrics collector started")

  runApplication<DaprSmartFactoryMetricsCollector>(*args)
}
