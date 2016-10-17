# Kafka REST API #

Version: 0.1-SNAPSHOT

#### REST service for Apache Kafka ####

### Prerequisites ###
* Zookeeper (for Kafka)
* Kafka 0.10.0.0

### Building ###
To make a jar you can do:  

`mvn package`

The jar file is then located under `target`.

### Running an instance ###
**Make sure your Kafka and Zookeeper servers are running first (see Kafka documentation)**

cd kafka-rest/bin
sh kafka-rest-start.sh

### Admin REST APIs ###
**Get all available brokers
**
`curl -X GET "http://localhost:8080/cluster/brokers"`

### Producer REST APIs ###

Producing JSON messages

`curl -X  POST -H "Content-Type: application/vnd.kafka.json.v1+json" --data  '[{"value":{"foo":"bar"}}]'  "http://localhost:8080/topics/MYTOPIC"`

`curl -X  POST -H "Content-Type: application/vnd.kafka.json.v1+json" --data  '[{"value":{"foo":"bar"}, "partition": 0}]'  "http://localhost:8080/topics/MYTOPIC"`

Producing Binary messages

`curl -X  POST -H "Content-Type: application/vnd.kafka.binary.v1+json" --data  '[{"key": "a2V5", "value": "Y29uZmx1ZW50"}]' "http://localhost:8080/topics/MYTOPIC"`

`curl -X  POST -H "Content-Type: application/vnd.kafka.binary.v1+json" --data  '[{"key": "a2V5", "value": "S2Fma2E=", "partition": 0}]' "http://localhost:8080/topics/MYTOPIC"`

`curl -X  POST -H "Content-Type: application/vnd.kafka.binary.v1+json" --data  '[{"key": "a2V5", "value": "S2Fma2E="},{"key": "a2V25", "value": "S2Fma2E="}]' "http://localhost:8080/topics/MYTOPIC/partitions/0"`

### Consumer REST APIs ###

**Consume from a partition**

`curl -X  GET -H "Accept: application/vnd.kafka.json.v1+json"   "http://localhost:8080/topics/MYTOPIC/partitions/0?offset=5"`

`curl -X  GET -H "Accept: application/vnd.kafka.binary.v1+json"   "http://localhost:8080/topics/MYTOPIC/partitions/1?offset=5&count=10"`

**Consumer Group Support**

TODO


### Example Kafka Rest Proxy Configuration (conf/kafka-rest.yml) ###
zookeeperHost: localhost:2181

producer:
  "bootstrap.servers": "localhost:9092"

consumer:
  "bootstrap.servers": "localhost:9092"
  "auto.offset.reset": "earliest"

restconfig:
  "id": "1"
  "simpleconsumer.poll.timeout": "1000"
  "simpleconsumer.pool.max.size": "10"
  "simpleconsumer.pool.instance.timeout": "1000"


logging:
  loggers:
    org.glassfish.jersey.filter.LoggingFilter: INFO
    org.apache.kafka.rest: DEBUG
