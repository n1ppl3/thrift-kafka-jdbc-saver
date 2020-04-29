package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class BlockingQueueUtilsTest {

    // поскольку за секунду продюсер производит 11 элементов
    private static final int ANY_NUMBER_MORE_THAN_ELEVEN = 12;
    private static final AtomicInteger producerNumber = new AtomicInteger();
    private static final AtomicInteger consumerNumber = new AtomicInteger();


    @Test
    void case_when_awaitCount_happens_earlier_than_timeout() throws InterruptedException {
        concurrentTest(10, 10, Duration.ofMillis(1_050));
    }

    @Test
    void case_when_timeout_happens_earlier_than_awaitCount() throws InterruptedException {
        concurrentTest(9, 11, Duration.ofMillis(850));
    }


    private static void concurrentTest(int expected, int awaitCount, Duration awaitTime) throws InterruptedException {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<>();

        Runnable runnable1 = () -> {
            try {
                cyclicBarrier.await();
                log.info("Producer thread started!");
                for (int i = 1; i <= ANY_NUMBER_MORE_THAN_ELEVEN; i++) {
                    log.info("Gonna add {}", i);
                    blockingQueue.add("" + i);
                    Thread.sleep(100);
                }
            } catch (InterruptedException | BrokenBarrierException e) {
                log.error(e.getLocalizedMessage(), e);
            }
        };
        Thread thread1 = new Thread(runnable1, "producer-" + producerNumber.incrementAndGet());
        thread1.start();

        List<String> outerElements = new ArrayList<>();
        Runnable runnable2 = () -> {
            try {
                cyclicBarrier.await();
                log.info("Consumer thread started!");
                var elements = BlockingQueueUtils.takeElementsWithinTime(blockingQueue, awaitCount, awaitTime);
                outerElements.addAll(elements);
            } catch (InterruptedException | BrokenBarrierException e) {
                log.error(e.getLocalizedMessage(), e);
            }
        };
        Thread thread2 = new Thread(runnable2, "consumer-" + consumerNumber.incrementAndGet());
        thread2.start();

        thread1.join();
        thread2.join();

        assertEquals(expected, outerElements.size());
    }

}
