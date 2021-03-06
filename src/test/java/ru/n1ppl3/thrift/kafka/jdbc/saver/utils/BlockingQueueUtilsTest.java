package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class BlockingQueueUtilsTest {

    // поскольку за секунду продюсер производит 10 элементов
    private static final int ANY_NUMBER_MORE_THAN_TEN = 12;
    private static final AtomicInteger producerNumber = new AtomicInteger();
    private static final AtomicInteger consumerNumber = new AtomicInteger();


    @Test
    void case_nothing_received() throws InterruptedException {
        concurrentTest(0, 3, Duration.ofMillis(50));
    }

    @Test
    void case_when_awaitCount_happens_earlier_than_timeout() throws InterruptedException {
        concurrentTest(10, 10, Duration.ofMillis(1_050));
    }

    @Test
    void case_when_timeout_happens_earlier_than_awaitCount() throws InterruptedException {
        concurrentTest(5, 7, Duration.ofMillis(550));
    }


    private static void concurrentTest(int expected, int awaitCount, Duration awaitTime) throws InterruptedException {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<>();

        Runnable runnable1 = () -> {
            try {
                cyclicBarrier.await();
                log.info("Producer thread started!");
                populateQueueSlowly(blockingQueue);
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


    @Test
    void check_when_interrupted_elements_returned_back_to_queue() throws Exception {
        BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<>();

        var executorService1 = Executors.newFixedThreadPool(1);
        var future1 = executorService1.submit(() -> {
            log.info("Consumer thread started!"); // указываем заведомо несбыточные значения
            return BlockingQueueUtils.takeElementsWithinTime(blockingQueue, 100, Duration.ofMillis(10_000));
        });

        var executorService2 = Executors.newFixedThreadPool(1);
        var future2 = executorService2.submit(() -> {
            log.info("Producer thread started!");
            populateQueueSlowly(blockingQueue);
            return executorService1.shutdownNow(); // посылаем в первый поток InterruptedException
        });

        var executionException = assertThrows(ExecutionException.class, () -> future1.get(10, TimeUnit.SECONDS));
        assertTrue(executionException.getCause() instanceof InterruptedException);

        assertEquals(Collections.emptyList(), future2.get(10, TimeUnit.SECONDS));
        assertEquals(ANY_NUMBER_MORE_THAN_TEN, blockingQueue.size());
    }


    private static void populateQueueSlowly(BlockingQueue<String> blockingQueue) throws InterruptedException {
        for (int i = 1; i <= ANY_NUMBER_MORE_THAN_TEN; i++) {
            Thread.sleep(100);
            log.info("Gonna add {}", i);
            blockingQueue.add("" + i);
        }
    }

}
