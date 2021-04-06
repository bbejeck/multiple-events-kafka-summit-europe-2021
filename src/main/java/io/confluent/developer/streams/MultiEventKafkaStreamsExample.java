package io.confluent.developer.streams;

import io.confluent.developer.avro.CustomerInfo;
import io.confluent.developer.avro.PageView;
import io.confluent.developer.avro.Purchase;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import io.confluent.developer.utils.PropertiesLoader;

import java.util.HashMap;
import java.util.Map;

public class MultiEventKafkaStreamsExample {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Must provide path to properties file for configurations");
            System.exit(1);
        }

        var streamsProperties = PropertiesLoader.load(args[0]);
        var mapConfigs = new HashMap<String, Object>();
        streamsProperties.forEach((k, v) -> mapConfigs.put((String) k, v));

        SpecificAvroSerde<CustomerInfo> customerSerde = getSpecificAvroSerde(mapConfigs);
        SpecificAvroSerde<SpecificRecord> specificAvroSerde = getSpecificAvroSerde(mapConfigs);

        String inputTopic = streamsProperties.getProperty("streams.input.topic.name");
        String outputTopic = streamsProperties.getProperty("streams.output.topic.name");

        StreamsBuilder builder = new StreamsBuilder();
        String storeName = "the_store";
        final StoreBuilder<KeyValueStore<String, CustomerInfo>> customerStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName),
                Serdes.String(),
                customerSerde);

        builder.addStateStore(customerStore);

        builder.stream(inputTopic, Consumed.with(Serdes.String(), specificAvroSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .transformValues(new EventValueTransformerSupplier(storeName), storeName)
                .peek((k, v) -> System.out.printf("Customer info %s %n", v))
                .to(outputTopic, Produced.with(Serdes.String(), customerSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProperties);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }


    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> configs) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(configs, false);
        return specificAvroSerde;
    }


    static class EventValueTransformerSupplier implements ValueTransformerWithKeySupplier<String, SpecificRecord, CustomerInfo> {
        private final String storename;

        public EventValueTransformerSupplier(String storename) {
            this.storename = storename;
        }

        @Override
        public ValueTransformerWithKey<String, SpecificRecord, CustomerInfo> get() {
            return new ValueTransformerWithKey<>() {
                private KeyValueStore<String, CustomerInfo> store;
                @Override
                public void init(ProcessorContext context) {
                        store = context.getStateStore(storename);
                }

                @Override
                public CustomerInfo transform(String readOnlyKey, SpecificRecord value) {
                    CustomerInfo customerInfo = store.get(readOnlyKey);
                    if (customerInfo == null) {
                        customerInfo = CustomerInfo.newBuilder().setCustomerId(readOnlyKey).build();
                    }
                    if (value instanceof PageView) {
                        PageView pageView = (PageView) value;
                        customerInfo.getPageViews().add(pageView.getUrl());

                    } else if (value instanceof Purchase) {
                        Purchase purchase = (Purchase) value;
                        customerInfo.getItems().add(purchase.getItem());
                    }
                    store.put(readOnlyKey, customerInfo);

                    return customerInfo;
                }

                @Override
                public void close() {

                }
            };
        }
    }
}
