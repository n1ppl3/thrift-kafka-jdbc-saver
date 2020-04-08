package ru.n1ppl3.thrift.kafka.jdbc.saver;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.thrift.TBase;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.ThriftSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static ru.n1ppl3.thrift.kafka.jdbc.saver.utils.Utils.sysGuid;


@Slf4j
public abstract class TestUtils {


    public static String generateRemoteAddr() {
        return "" + Instant.now().toEpochMilli();
    }


    public static <T extends TBase<T,?>> void sendToListener(String bootstrapServers, String topic, T object) throws ExecutionException, InterruptedException {
        KafkaTemplate<String, T> kafkaTemplate = kafkaTemplate(bootstrapServers);
        log.info("sent {}", kafkaTemplate.send(topic, sysGuid(), object).get());
        kafkaTemplate.flush();
    }


    public static <V extends TBase<V,?>> KafkaTemplate<String, V> kafkaTemplate(String bootstrapServers) {
        Map<String, Object> producerConfig = new LinkedHashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        Serializer<V> valueSerializer = new ThriftSerializer<V>();
        var producerFactory = new DefaultKafkaProducerFactory<>(producerConfig, new StringSerializer(), valueSerializer);

        var kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setCloseTimeout(Duration.ofMillis(1_000));
        return kafkaTemplate;
    }

}
