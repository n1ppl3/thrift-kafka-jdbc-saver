package ru.n1ppl3.thrift.kafka.jdbc.saver.configuration;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.LogIfLevelEnabled;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.TUserLogRecordV1;
import ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.ThriftDeserializer;

import static org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE;

@EnableKafka
@Configuration
@AllArgsConstructor
@Import({ConfigurationPropertiesAutoConfiguration.class, KafkaProperties.class})
public class SpringKafkaConfiguration {

    private final KafkaProperties kafkaProperties;

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
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(), stringDeserializer, deserializer);
    }
}
