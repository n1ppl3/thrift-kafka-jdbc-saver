package ru.n1ppl3.thrift.kafka.jdbc.saver.configuration;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.LogIfLevelEnabled;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.TUserLogRecordV1;
import ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.ThriftDeserializer;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST;
import static org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE;

@EnableKafka
@Configuration
@AllArgsConstructor
public class SpringKafkaConfiguration {

    private final Environment env;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, TUserLogRecordV1>();
        factory.getContainerProperties().setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        factory.getContainerProperties().setAckMode(MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(1_000);
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.setConcurrency(2);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, TUserLogRecordV1> consumerFactory() {
        Deserializer<String> stringDeserializer = new StringDeserializer();
        Deserializer<TUserLogRecordV1> deserializer = new ThriftDeserializer<>(TUserLogRecordV1.class);
        return new DefaultKafkaConsumerFactory<>(consumerProperties(env), stringDeserializer, deserializer);
    }

    public static Map<String, Object> consumerProperties(Environment env) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getRequiredProperty("spring.kafka.bootstrap-servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, env.getRequiredProperty("spring.kafka.consumer.group-id"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, LATEST.name().toLowerCase(Locale.US));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return props;
    }
}
