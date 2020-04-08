package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka;

import lombok.Getter;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.lang.NonNull;

import java.util.concurrent.CountDownLatch;


public class ConsumerStartedAppListener implements ApplicationListener<KafkaEvent> {

    @Getter
    private final CountDownLatch countDownLatch = new CountDownLatch(2); // ConcurrentKafkaListenerContainerFactory concurrency

    @Override
    public void onApplicationEvent(@NonNull KafkaEvent event) {
        System.err.println(" <<< - - - - - - - - " + event + " - - - - - - - - >>> ");
        if (event instanceof ConsumerStartedEvent) {
            countDownLatch.countDown();
        }
    }

}
