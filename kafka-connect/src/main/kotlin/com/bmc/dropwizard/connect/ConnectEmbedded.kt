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
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore
import org.apache.kafka.connect.util.FutureCallback
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This is only a temporary extension to Kafka Connect runtime until there is an Embedded API as per KIP-26

 * borrowed from https://github.com/amient/hello-kafka-streams/blob/master/src/main/java/org/apache/kafka/connect/api/ConnectEmbedded.java
 */

class ConnectEmbedded @Throws(Exception::class)
constructor(workerConfig: WorkerConfig, private val connectorConfigs: List<Map<String, String>>) : Managed {

    private val worker: Worker
    private val herder: Herder
    private val startLatch = CountDownLatch(1)
    private val shutdown = AtomicBoolean(false)
    private val stopLatch = CountDownLatch(1)

    init {

        if (workerConfig is DistributedConfig) {
            // if distributed.. could also use File/Memory OffsetBackingStore
            val offsetBackingStore = KafkaOffsetBackingStore()
            offsetBackingStore.configure(workerConfig.originalsStrings())
            worker = Worker(workerConfig, offsetBackingStore)
            herder = DistributedHerder(workerConfig, worker, "/connect")
        } else {

            val offsetBackingStore = FileOffsetBackingStore()
            offsetBackingStore.configure(workerConfig.originalsStrings())
            worker = Worker(workerConfig, offsetBackingStore)
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
}