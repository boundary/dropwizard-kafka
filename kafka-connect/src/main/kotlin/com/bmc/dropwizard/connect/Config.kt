package com.bmc.dropwizard.connect

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.io.Resources
import com.google.common.net.HostAndPort
import io.dropwizard.setup.Environment
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig
import org.hibernate.validator.constraints.NotEmpty
import java.util.*

class ConnectConfiguration {

    enum class MODE {
        standalone, distributed
    }

    val defaultPort = 9992

    @JsonProperty
    var mode = MODE.standalone

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
        return when (mode) {
            MODE.standalone -> StandaloneConfig(p)
            MODE.distributed -> DistributedConfig(p)
        }
    }

    fun createConnector(environment: Environment): ConnectEmbedded {
        var embedded = ConnectEmbedded(workerConfig(), connectorConfigs())
        environment.lifecycle().manage(embedded)
        return embedded;
    }

    private fun resourceAsProperties(resourceName: String): MutableMap<String, String> {
        val url = Resources.getResource(resourceName)
        val p = Properties()
        p.load(url.openStream())
        return Utils.propsToStringMap(p)
    }

}

