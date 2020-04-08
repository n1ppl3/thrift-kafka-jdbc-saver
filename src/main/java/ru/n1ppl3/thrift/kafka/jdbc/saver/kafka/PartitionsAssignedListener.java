package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class PartitionsAssignedListener implements ConsumerRebalanceListener {

    @Getter
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("ON_PARTITIONS_REVOKED: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("ON_PARTITIONS_ASSIGNED: {}", partitions);
        countDownLatch.countDown();
    }

}
