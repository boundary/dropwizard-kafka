# dropwizard-kafka

Some utilities for embedding kafka-connect within dropwizard. 

Basically takes the ideas from http://www.confluent.io/blog/hello-world-kafka-connect-kafka-streams,
and wraps them into a dropwizard-friendly configuration format, and hooks into the dropwizard environment lifecycle.

# usage


.. reference the project (until this is published to maven central, you will need to clone this repo and run `mvn install`)

```
        <dependency>
            <groupId>com.bmc.dropwizard</groupId>
            <artifactId>kafka-connect</artifactId>
            <version>${dropwizard.connnect.version}</version>
        </dependency>

```

.. reference `ConnectorConfiguration` in your config file

```java

    @Valid
    @JsonProperty
    var kafka = ConnectConfiguration();

```

.. put your connector and worker.properties files in your resources folder.
.. see http://docs.confluent.io/2.0.0/connect/userguide.html#configuring-connectors for info about this
.. create your Connector and Task files (again, see http://docs.confluent.io/2.0.0/connect/devguide.html for help here)



# todo

publish to mvn central
suport filesystem and resource configuration
upgrade to kafka 0.10
support kafka-streams
report health + metrics 

