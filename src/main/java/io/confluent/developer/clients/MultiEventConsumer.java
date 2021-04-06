package io.confluent.developer.clients;

import io.confluent.developer.avro.CustomerEvent;
import io.confluent.developer.avro.PageView;
import io.confluent.developer.avro.Purchase;
import io.confluent.developer.proto.CustomerEventProto;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.developer.utils.PropertiesLoader;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MultiEventConsumer {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Must provide path to properties file for configurations");
            System.exit(1);
        }
        var consumerProperties = PropertiesLoader.load(args[0]);
        var consumerConfigs = new HashMap<String, Object>();
        consumerProperties.forEach((k, v) -> consumerConfigs.put((String) k, v));

        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumeAvroUnwrappedRecords(consumerConfigs);
        consumeAvroSpecificRecords(consumerConfigs);
        consumeProtobufRecords(consumerConfigs);

    }

    static void consumeAvroUnwrappedRecords(final Map<String, Object> baseConfigs) {
        var consumerConfigs = new HashMap<>(baseConfigs);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "unwrapped-avro-group");
        try (final Consumer<String, SpecificRecord> unwrappedConsumer = new KafkaConsumer<>(consumerConfigs)) {
            final String topicName = (String) consumerConfigs.get("avro.topic");
            unwrappedConsumer.subscribe(Collections.singletonList(topicName));
            ConsumerRecords<String, SpecificRecord> records = unwrappedConsumer.poll(Duration.ofSeconds(5));
            records.forEach(record -> handleAvroRecord(record.value()));
        }
    }

    static void consumeAvroSpecificRecords(final Map<String, Object> baseConfigs) {
        var consumerConfigs = new HashMap<>(baseConfigs);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "wrapped-avro-group");
        try (final Consumer<String, CustomerEvent> specificConsumer = new KafkaConsumer<>(consumerConfigs)) {
            final String topicName = (String) consumerConfigs.get("avro.wrapped.topic");
            specificConsumer.subscribe(Collections.singletonList(topicName));
            ConsumerRecords<String, CustomerEvent> records = specificConsumer.poll(Duration.ofSeconds(5));
            records.forEach(record -> {
                final CustomerEvent customerEvent = record.value();
                System.out.printf("[Avro] Found a CustomerRecord event %s %n", customerEvent);
                SpecificRecord action = (SpecificRecord) customerEvent.getAction();
                handleAvroRecord(action);
            });
        }
    }

    private static void handleAvroRecord(final SpecificRecord avroRecord) {
        if (avroRecord instanceof PageView) {
            PageView pageView = (PageView) avroRecord;
            System.out.printf("[Avro] Found an embedded PageView event %s %n", pageView);
        } else if (avroRecord instanceof Purchase) {
            Purchase purchase = (Purchase) avroRecord;
            System.out.printf("[Avro] Found an Avro embedded Purchase event %s %n", purchase);
        } else {
            throw new IllegalStateException(String.format("Unrecognized type %s %n", avroRecord.getSchema().getFullName()));
        }
    }

    static void consumeProtobufRecords(final Map<String, Object> baseConfigs) {
        var consumerConfigs = new HashMap<>(baseConfigs);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        consumerConfigs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, CustomerEventProto.CustomerEvent.class);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "proto-group");
        try (final Consumer<String, CustomerEventProto.CustomerEvent> protoConsumer = new KafkaConsumer<>(consumerConfigs)) {
            final String topicName = (String) consumerConfigs.get("proto.topic");
            protoConsumer.subscribe(Collections.singletonList(topicName));
            ConsumerRecords<String, CustomerEventProto.CustomerEvent> records = protoConsumer.poll(Duration.ofSeconds(5));
            records.forEach(record -> {
                final CustomerEventProto.CustomerEvent customerEvent = record.value();
                CustomerEventProto.CustomerEvent.ActionCase actionCase = customerEvent.getActionCase();
                switch (actionCase) {
                    case PURCHASE:
                        System.out.printf("[Protobuf] Found a Purchase %s %n", customerEvent.getPurchase());
                        break;
                    case PAGE_VIEW:
                        System.out.printf("[Protobuf] Found a PageView %s %n", customerEvent.getPageView());
                        break;
                    case ACTION_NOT_SET:
                        System.out.println("[Protobuf] Customer action not set");
                        break;
                }
            });
        }
    }
}
