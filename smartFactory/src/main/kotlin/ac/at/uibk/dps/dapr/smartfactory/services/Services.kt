package ac.at.uibk.dps.dapr.smartfactory.services

import ac.at.uibk.dps.dapr.smartfactory.api.ForyConfig
import ac.at.uibk.dps.dapr.smartfactory.api.MessageProcessingRequest
import ac.at.uibk.dps.dapr.smartfactory.api.PhotoScanRequest
import ac.at.uibk.dps.dapr.smartfactory.api.StatisticsRequest
import ac.at.uibk.dps.dapr.smartfactory.metrics
import ac.at.uibk.dps.dapr.smartfactory.utils.Utils
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import org.apache.fory.memory.MemoryBuffer
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

object Services {

  private val fory = ForyConfig.fory

  private val threadBuffer = ThreadLocal.withInitial { MemoryBuffer.newHeapBuffer(1024) }
  private val client = HttpClient.newHttpClient()
  var baseUrl = System.getenv("SERVICE_URL") ?: "http://localhost:6000"

  val invokeTimer = metrics.timer("invoke.time")

  fun processEmail(req: MessageProcessingRequest): Mono<Void> {
    val startTime = System.nanoTime()

    val buffer = threadBuffer.get().apply { writerIndex(0) }
    fory.serialize(buffer, req)

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/process/email"))
        .header("Content-Type", "application/x-fury")
        .POST(HttpRequest.BodyPublishers.ofByteArray(buffer.getBytes(0, buffer.writerIndex())))
        .build()

    client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).whenComplete { _,_ ->
      val invokeTime = System.nanoTime() - startTime
      invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
    }

    return Mono.empty()
  }

  fun processSms(req: MessageProcessingRequest): Mono<Void> {
    val startTime = System.nanoTime()

    val buffer = threadBuffer.get().apply { writerIndex(0) }
    fory.serialize(buffer, req)

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/process/sms"))
        .header("Content-Type", "application/x-fury")
        .POST(HttpRequest.BodyPublishers.ofByteArray(buffer.getBytes(0, buffer.writerIndex())))
        .build()

    client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).whenComplete { _,_ ->
      val invokeTime = System.nanoTime() - startTime
      invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
    }

    return Mono.empty()
  }

  fun sendStatistics(req: StatisticsRequest): Mono<Void> {
    val startTime = System.nanoTime()

    val buffer = threadBuffer.get().apply { writerIndex(0) }
    fory.serialize(buffer, req)

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/statistics"))
        .header("Content-Type", "application/x-fury")
        .POST(HttpRequest.BodyPublishers.ofByteArray(buffer.getBytes(0, buffer.writerIndex())))
        .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept { response ->
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
        }
    )
  }

  fun takePhoto(): Mono<Void> {
    val startTime = System.nanoTime()

    val request = HttpRequest.newBuilder().uri(URI.create("$baseUrl/takephoto")).GET().build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept { response ->
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }

  fun scanPhoto(req: PhotoScanRequest): Mono<Void> {
    val startTime = System.nanoTime()

    val buffer = threadBuffer.get().apply { writerIndex(0) }
    fory.serialize(buffer, req)

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/scanphoto"))
        .header("Content-Type", "application/x-fury")
        .POST(HttpRequest.BodyPublishers.ofByteArray(buffer.getBytes(0, buffer.writerIndex())))
        .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenAccept { response ->
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }

  fun discardObject(): Mono<Void> {
    val startTime = System.nanoTime()

    val request = HttpRequest.newBuilder().uri(URI.create("$baseUrl/discardobject")).GET().build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept { response ->
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }

  fun moveBelt(): Mono<Void> {
    val startTime = System.nanoTime()

    val request = HttpRequest.newBuilder().uri(URI.create("$baseUrl/movebelt")).GET().build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept { response ->
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }

  fun stopBelt(): Mono<Void> {
    val startTime = System.nanoTime()

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/stopbelt"))
        .GET()
        .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept {
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }

  fun pickUp(): Mono<Void> {
    val startTime = System.nanoTime()

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/pickup"))
        .GET()
        .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenAccept {
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }

  fun assemble(): Mono<Void> {
    val startTime = System.nanoTime()

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/assemble"))
        .GET()
        .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenAccept {
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }

  fun returnToStart(): Mono<Void> {
    val startTime = System.nanoTime()

    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("$baseUrl/returntostart"))
        .GET()
        .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept {
        val invokeTime = System.nanoTime() - startTime
        invokeTimer.update(invokeTime, TimeUnit.NANOSECONDS)
      }
    )
  }
}
