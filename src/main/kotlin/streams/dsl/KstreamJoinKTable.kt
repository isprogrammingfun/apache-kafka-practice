package org.example.streams.dsl

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.*

class KstreamJoinKTable {
    companion object {
        private const val APPLICATION_NAME = "streams-join-application"
        private const val BOOTSTRAP_SERVERS = "my-kafka:9092"
        private const val ADDRESS_TABLE = "address"
        private const val ORDER_STREAM = "order"
        private const val ORDER_JOIN_STREAM = "order_join"
    }

    fun main(args: Array<String>) {
        val properties = Properties()

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)

        val builder = StreamsBuilder()
        val addressTable = builder.table<String, String>(ADDRESS_TABLE)
        val orderStream = builder.stream<String, String>(ORDER_STREAM)

        orderStream.join(addressTable) { order, address -> "$order send to $address" }
            .to(ORDER_JOIN_STREAM)

        val topology = builder.build()
        val streams = KafkaStreams(topology, properties)
        streams.start()

    }
}