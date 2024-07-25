package streams.processor

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import java.util.*


class SimpleKafkaProcessor {
    companion object {
        private const val APPLICATION_NAME = "processor-application"
        private const val BOOTSTRAP_SERVERS = "my-kafka:9092"
        private const val STREAM_LOG = "stream_log"
        private const val STREAM_LOG_FILTER = "stream_log_filter"
    }

    fun main(args: Array<String>) {
        val properties = Properties()
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)

        val topology = Topology()
        topology.addSource("Source", STREAM_LOG)
            .addProcessor("Process", { FilterProcessor() }, "Source")
            .addSink("Sink", STREAM_LOG_FILTER, "Process")

        val streams = KafkaStreams(topology, properties)
        streams.start()
    }
}