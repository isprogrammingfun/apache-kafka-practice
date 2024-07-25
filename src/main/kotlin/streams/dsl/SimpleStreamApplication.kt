package org.example.streams.dsl

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.*

class SimpleStreamApplication {
    companion object {
        private const val APPLICATION_NAME = "streams-application"
        private const val BOOTSTRAP_SERVERS = "my-kafka:9092"
        private const val STREAM_LOG = "stream_log"
        private const val STREAM_LOG_COPY = "stream_log_copy"
    }

    fun main(args: Array<String>) {
        val properties = Properties()
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)

        val builder = StreamsBuilder()
        val streamLog = builder.stream<String, String>(STREAM_LOG)
        streamLog.to(STREAM_LOG_COPY)

        val topology = builder.build()
        val streams = KafkaStreams(topology, properties)
        streams.start()
    }
}