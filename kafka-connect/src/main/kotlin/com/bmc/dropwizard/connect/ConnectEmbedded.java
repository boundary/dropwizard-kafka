package com.bmc.dropwizard.connect;


import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is only a temporary extension to Kafka Connect runtime until there is an Embedded API as per KIP-26
 *
 * borrowed from https://github.com/amient/hello-kafka-streams/blob/master/src/main/java/org/apache/kafka/connect/api/ConnectEmbedded.java
 */

public class ConnectEmbedded implements Managed {
    private static final Logger log = LoggerFactory.getLogger(ConnectEmbedded.class);
    private static final int REQUEST_TIMEOUT_MS = 120000;

    private final Worker worker;
    private final Herder herder;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private final List<Map<String, String>> connectorConfigs;

    public ConnectEmbedded(WorkerConfig workerConfig, List<Map<String, String>> connectorConfigs) throws Exception {

        Time time = new SystemTime();



        //not sure if this is going to work but because we don't have advertised url we can get at least a fairly random

        String workerId = UUID.randomUUID().toString();

        if (workerConfig instanceof DistributedConfig) {
            // if distributed.. could also use File/Memory OffsetBackingStore
            KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
            offsetBackingStore.configure(workerConfig.originalsStrings());
            worker = new Worker(workerConfig, offsetBackingStore);
            herder = new DistributedHerder((DistributedConfig) workerConfig, worker, "/connect");
        } else {

            FileOffsetBackingStore offsetBackingStore = new FileOffsetBackingStore();
            offsetBackingStore.configure(workerConfig.originalsStrings());
            worker = new Worker(workerConfig, offsetBackingStore);
            herder = new StandaloneHerder(worker);
        }

        //advertisedUrl = "" as we don't have the rest server - hopefully this will not break anything
        this.connectorConfigs = connectorConfigs;
    }

    @Override
    public void start() {
        try {
            log.info("Kafka ConnectEmbedded starting");

            worker.start();
            herder.start();

            log.info("Kafka ConnectEmbedded started");

            for (Map<String, String> connectorConfig : connectorConfigs) {
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
                String name = connectorConfig.get(ConnectorConfig.NAME_CONFIG);
                herder.putConnectorConfig(name, connectorConfig, true, cb);
                cb.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }

        } catch (InterruptedException e) {
            log.error("Starting interrupted ", e);
        } catch (ExecutionException e) {
            log.error("Submitting connector config failed", e.getCause());
        } catch (TimeoutException e) {
            log.error("Submitting connector config timed out", e);
        } finally {
            startLatch.countDown();
        }
    }

    @Override
    public void stop() throws InterruptedException {
        try {

            startLatch.await();
            boolean wasShuttingDown = shutdown.getAndSet(true);
            if (!wasShuttingDown) {

                log.info("Kafka ConnectEmbedded stopping");
                herder.stop();
                worker.stop();

                log.info("Kafka ConnectEmbedded stopped");
            }
        } finally {
            stopLatch.countDown();
        }
    }
}