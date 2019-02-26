package com.example.demo;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;

public class FifoTest {
  private static final Logger LOG = LoggerFactory.getLogger(FifoTest.class);
  private static final String TEST_STORAGE = "test-storage";
  private static final String TEST_APP = "TestApp";
  private static final String TEST_SOURCE_NAME = "TestSource";
  private static final String TEST_PROCESSOR_NAME = "TestProcessor";
  private static final String SOURCE_TOPIC = "inbox-message-added";
  private static final String SINK_NAME = "TestSink";
  private static final String SINK_TOPIC = "test-sink";
  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final JsonSerde<DemoEvent> DEMO_EVENT_SERDE = new JsonSerde<>(DemoEvent.class);
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  private class TestProcessor implements Processor<String, DemoEvent> {
    private final CountDownLatch latch;
    private KeyValueStore<String, DemoEvent> stateStore;
    private ProcessorContext context;

    TestProcessor(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
      this.stateStore = (KeyValueStore<String, DemoEvent>) context.getStateStore(TEST_STORAGE);
    }

    @Override
    public void process(String key, DemoEvent value) {
      if (key.equals("1234567890123456")) {
        stateStore.flush();
        context.commit();
        latch.countDown();
      } else {
        stateStore.put(key, value);
        if (key.contains("00000")) {
          LOG.info("Processing key:[{}] value:[{}]", key, stateStore.get(key));
          context.commit();
        }
      }
    }

    @Override
    public void close() {
    }
  }

  @Test
  public void testStorageFifo() {
    CountDownLatch latch = new CountDownLatch(1);
    StreamsBuilder builder = new StreamsBuilder();
    Topology topology = builder.build();
    registerTopology(topology, () -> new TestProcessor(latch), Stores.persistentKeyValueStore(TEST_STORAGE));
    Map<String, Object> kafkaConfig = getKafkaConfigMap();
    KafkaProducer<String, DemoEvent> kafkaProducer = new KafkaProducer<>(kafkaConfig);
    KafkaStreams streams = getKafkaStreams(topology, kafkaConfig);
    streams.start();

    LOG.info("Start producing");
    LongStream.range(0, 1_000_000).forEach(
        i -> {
          ProducerRecord<String, DemoEvent> record = new ProducerRecord<>(SOURCE_TOPIC, String.valueOf(i), createDemoEvent());
          kafkaProducer.send(record);
        }
    );
    LOG.info("Finished producing");
    ProducerRecord<String, DemoEvent> record = new ProducerRecord<>(SOURCE_TOPIC, "1234567890123456", createDemoEvent());
    kafkaProducer.send(record);

    try {
      latch.await();
    } catch (InterruptedException e) {
      LOG.error("Processing interrupted", e);
    } finally {
      streams.close();
      kafkaProducer.close();
    }
  }

  private DemoEvent createDemoEvent() {
    return new DemoEvent(randomString(10), randomString(40), randomLong(), UUID.randomUUID());
  }

  private String randomString(int size) {
    byte[] array = new byte[size];
    new Random().nextBytes(array);
    return new String(array, StandardCharsets.UTF_8);
  }

  private Long randomLong() {
    return new Random().nextLong();
  }

  private void registerTopology(Topology topology, ProcessorSupplier processor, KeyValueBytesStoreSupplier store) {
    topology
        .addSource(TEST_SOURCE_NAME, STRING_SERDE.deserializer(), DEMO_EVENT_SERDE.deserializer(), SOURCE_TOPIC)
        .addProcessor(TEST_PROCESSOR_NAME, processor, TEST_SOURCE_NAME)
        .addStateStore(Stores.keyValueStoreBuilder(store, STRING_SERDE, DEMO_EVENT_SERDE), TEST_PROCESSOR_NAME)
        .addSink(SINK_NAME, SINK_TOPIC, STRING_SERDE.serializer(), DEMO_EVENT_SERDE.serializer(), TEST_PROCESSOR_NAME);
  }

  private static KafkaStreams getKafkaStreams(Topology topology, Map<String, Object> kafkaConfig) {
    Properties properties = new Properties();
    properties.putAll(kafkaConfig);
    return new KafkaStreams(topology, properties);
  }

  private Map<String, Object> getKafkaConfigMap() {
    final Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, TEST_APP);
    kafkaConfig.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEMO_EVENT_SERDE.serializer().getClass());
    kafkaConfig.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, SimpleRocksDbConfigurationSetter.class);
    return kafkaConfig;
  }

}
