package com.bmc.dropwizard.connect

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.io.Resources
import com.google.common.net.HostAndPort
import io.dropwizard.setup.Environment
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.connect.runtime.Herder
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource
import org.apache.kafka.connect.runtime.rest.resources.RootResource
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig
import org.hibernate.validator.constraints.NotEmpty
import java.util.*

/**
 *  Kafka Connection Configuration
 */
class ConnectConfiguration {

    enum class MODE {
        standalone, distributed
    }

    val defaultPort = 9992

    @JsonProperty
    var mode = MODE.standalone

    @JsonProperty
    var restPort: Int? = null;

    @JsonProperty
    var restHost: String? = null;


    @JsonProperty
    @NotEmpty
    var workerProps = "kafka-connect/${mode}.worker.properties"


    @JsonProperty
    @NotEmpty
    var connectorProps = listOf(
            "kafka-connect/connector.properties"
    )

    @JsonProperty
    var bootstrapServers: List<HostAndPort> = listOf();

    fun connectorConfigs(): List<Map<String, String>> {
        return connectorProps
                .map { resourceAsProperties(it) }

    }

    fun workerConfig(): WorkerConfig {
        val p = resourceAsProperties(workerProps)
        if (bootstrapServers.isNotEmpty()) {
            val servers = bootstrapServers
                .map {
                    val port = it.getPortOrDefault(defaultPort)
                    "${it.hostText}:$port"
                }.joinToString(",")
            p.put("bootstrap.servers", servers)
        }

        with(restHost){
            if (this != null) {
                p.put("rest.host.name", this)
            }
        }
        with(restPort) {
            if (this != null) {
                p.put("rest.port", this.toString())
            }
        }

        return when (mode) {
            MODE.standalone -> StandaloneConfig(p)
            MODE.distributed -> DistributedConfig(p)
        }
    }

    fun createConnector(environment: Environment): ConnectEmbedded {
        val embedded = ConnectEmbedded(workerConfig(), connectorConfigs())
        environment.lifecycle().manage(embedded)

        wireRestAPI(environment, embedded.herder)

        return embedded;
    }

    private fun wireRestAPI(environment: Environment, herder: Herder) {
        // TODO enable cors support
        // also to consider: host these resources under a configurable path
        // or maybe host them on the admin connector

        environment.jersey().register(RootResource::class.java)
        environment.jersey().register(ConnectorsResource(herder))
        environment.jersey().register(ConnectorPluginsResource(herder))
        environment.jersey().register(ConnectExceptionMapper::class.java)

    }

    private fun resourceAsProperties(resourceName: String): MutableMap<String, String> {
        val url = Resources.getResource(resourceName)
        val p = Properties()
        p.load(url.openStream())
        return Utils.propsToStringMap(p)
    }

}

