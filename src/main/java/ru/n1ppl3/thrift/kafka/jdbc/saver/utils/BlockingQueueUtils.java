package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class BlockingQueueUtils {


    public static <T> List<T> takeElementsWithinTime(BlockingQueue<T> blockingQueue, int count, Duration timeout) throws InterruptedException {
        return takeElementsWithinTime(blockingQueue, count, timeout.toNanos());
    }


    public static <T> List<T> takeElementsWithinTime(BlockingQueue<T> blockingQueue, int count, long timeout, TimeUnit timeUnit) throws InterruptedException {
        return takeElementsWithinTime(blockingQueue, count, timeUnit.toNanos(timeout));
    }


    /**
     * method polls provided blockingQueue until time elapsed or elements count reached
     */
    public static <T> List<T> takeElementsWithinTime(BlockingQueue<T> blockingQueue, int count, long timeoutNano) throws InterruptedException {
        List<T> result = new ArrayList<>(count);

        while (timeoutNano > 0 && result.size() < count) {
            long start = System.nanoTime();
            T element;
            try {
                element = blockingQueue.poll(timeoutNano, TimeUnit.NANOSECONDS);
            } catch (InterruptedException ie) {
                log.warn("Interrupted, so gonna return {} element(s) back to queue", result.size());
                result.forEach(t -> {
                    if (!blockingQueue.offer(t)) {
                        log.warn("Queue is full, so failed to return back {}", t);
                    }
                });
                throw ie;
            }
            long stop = System.nanoTime();
            //logger.info("start={}; stop={}; diff={}; element={}", start, stop, (stop - start), element);

            if (element == null) {
                break; // returns the head of this queue, or null if the specified waiting time elapses before an element is available
            } else {
                result.add(element);
                timeoutNano -= stop - start;
                //logger.info("timeoutNano={}; result.size={}", timeoutNano, result.size());
            }
        }

        return result;
    }

}
