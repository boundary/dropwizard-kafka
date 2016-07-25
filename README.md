# dropwizard-kafka

Some utilities for embedding kafka-connect within dropwizard. 

Basically takes the ideas from http://www.confluent.io/blog/hello-world-kafka-connect-kafka-streams,
and wraps them into a dropwizard-friendly configuration format, and hooks into the dropwizard environment lifecycle.

# usage


.. reference the project

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

  * [x] publish to mvn central
  * [ ] support filesystem and resource configuration
  * [ ] upgrade to kafka 0.10
  * [ ] support kafka-streams
  * [ ] report health + metrics 
  * [ ] expose /connect uri

# LICENSE

Copyright 2016 BMC

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
