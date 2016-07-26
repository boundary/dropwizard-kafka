package com.bmc.dropwizard.connect


import io.dropwizard.lifecycle.Managed
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.Herder
import org.apache.kafka.connect.runtime.Worker
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.distributed.DistributedHerder
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder
import org.apache.kafka.connect.storage.FileOffsetBackingStore
import org.apache.kafka.connect.storage.KafkaConfigBackingStore
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore
import org.apache.kafka.connect.storage.KafkaStatusBackingStore
import org.apache.kafka.connect.util.FutureCallback
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import javax.ws.rs.core.UriBuilder

/**
 * This is only a temporary extension to Kafka Connect runtime until there is an Embedded API as per KIP-26

 * borrowed from https://github.com/amient/hello-kafka-streams/blob/master/src/main/java/org/apache/kafka/connect/api/ConnectEmbedded.java
 */

class ConnectEmbedded @Throws(Exception::class)
constructor(workerConfig: WorkerConfig, private val connectorConfigs: List<Map<String, String>>) : Managed {

    private val worker: Worker
    val herder: Herder
    private val startLatch = CountDownLatch(1)
    private val shutdown = AtomicBoolean(false)
    private val stopLatch = CountDownLatch(1)

    init {

       // worker id
        val advertisedUrl = advertisedUrl(workerConfig)
        val workerId: String = advertisedUrl.host + ":" + advertisedUrl.port

        val time = SystemTime()
        if (workerConfig is DistributedConfig) {
            // if distributed.. could also use File/Memory OffsetBackingStore

            val offsetBackingStore = KafkaOffsetBackingStore()
            offsetBackingStore.configure(workerConfig)

            worker = Worker(workerId, time, workerConfig, offsetBackingStore)

            val statusBackingStore = KafkaStatusBackingStore(time, worker.internalValueConverter)
            statusBackingStore.configure(workerConfig)

            val configBackingStore = KafkaConfigBackingStore(worker.internalValueConverter)
            configBackingStore.configure(workerConfig)

            // TODO fix url pass in from dw config
            herder = DistributedHerder(workerConfig, time, worker, statusBackingStore, configBackingStore, advertisedUrl.toString())

        } else {

            val offsetBackingStore = FileOffsetBackingStore()
            offsetBackingStore.configure(workerConfig)
            worker = Worker(workerId, time, workerConfig, offsetBackingStore)
            herder = StandaloneHerder(worker)
        }
    }

    override fun start() {
        try {
            log.info("Kafka ConnectEmbedded starting")

            worker.start()
            herder.start()

            log.info("Kafka ConnectEmbedded started")

            for (connectorConfig in connectorConfigs) {
                val cb = FutureCallback<Herder.Created<ConnectorInfo>>()
                val name = connectorConfig[ConnectorConfig.NAME_CONFIG]
                herder.putConnectorConfig(name, connectorConfig, true, cb)
                cb.get(REQUEST_TIMEOUT_MS.toLong(), TimeUnit.MILLISECONDS)
            }

        } catch (e: InterruptedException) {
            log.error("Starting interrupted ", e)
        } catch (e: ExecutionException) {
            log.error("Submitting connector config failed", e.cause)
        } catch (e: TimeoutException) {
            log.error("Submitting connector config timed out", e)
        } finally {
            startLatch.countDown()
        }
    }

    @Throws(InterruptedException::class)
    override fun stop() {
        try {

            startLatch.await()
            val wasShuttingDown = shutdown.getAndSet(true)
            if (!wasShuttingDown) {

                log.info("Kafka ConnectEmbedded stopping")
                herder.stop()
                worker.stop()

                log.info("Kafka ConnectEmbedded stopped")
            }
        } finally {
            stopLatch.countDown()
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ConnectEmbedded::class.java)
        private val REQUEST_TIMEOUT_MS = 120000
    }

    fun advertisedUrl(config: WorkerConfig): URI {

        val hostname = config.getString(WorkerConfig.REST_HOST_NAME_CONFIG)
        val port = config.getInt(WorkerConfig.REST_PORT_CONFIG)

        val uri: URI = URI("http://$hostname:$port")
        val builder = UriBuilder.fromUri(uri)

        val advertisedHostname = config.getString(WorkerConfig.REST_ADVERTISED_HOST_NAME_CONFIG)
        if (advertisedHostname != null && !advertisedHostname.isEmpty())
            builder.host(advertisedHostname)
        val advertisedPort = config.getInt(WorkerConfig.REST_ADVERTISED_PORT_CONFIG)
        if (advertisedPort != null)
            builder.port(advertisedPort)
        else
            builder.port(config.getInt(WorkerConfig.REST_PORT_CONFIG)!!)
        return builder.build()
    }
}