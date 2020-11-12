package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/*

EntityLocker (task from ALM Works)
------------

The task is to create a reusable utility class that provides synchronization mechanism similar to row-level DB locking.

The class is supposed to be used by the components that are responsible for managing storage and caching of different type of entities in the application. EntityLocker itself does not deal with the entities, only with the IDs (primary keys) of the entities.

Requirements:

1. EntityLocker should support different types of entity IDs.

2. EntityLocker’s interface should allow the caller to specify which entity does it want to work with (using entity ID), and designate the boundaries of the code that should have exclusive access to the entity (called “protected code”).

3. For any given entity, EntityLocker should guarantee that at most one thread executes protected code on that entity. If there’s a concurrent request to lock the same entity, the other thread should wait until the entity becomes available.

4. EntityLocker should allow concurrent execution of protected code on different entities.


Bonus requirements (optional):

I. Allow reentrant locking.

II. Allow the caller to specify timeout for locking an entity.

III. Implement protection from deadlocks (but not taking into account possible locks outside EntityLocker).

IV. Implement global lock. Protected code that executes under a global lock must not execute concurrently with any other protected code.

V. Implement lock escalation. If a single thread has locked too many entities, escalate its lock to be a global lock.

 */
@Slf4j
public abstract class EntityLocker {

    // interfaces

    public interface EntityId {}

    public interface Entity {
        EntityId getId();
    }

    // Locker itself

    private static final ConcurrentHashMap<EntityId, Lock> concurrentHashMap = new ConcurrentHashMap<>();

    public static void exclusivelyDoWithEntity(@NonNull Entity entity, @NonNull Consumer<Entity> entityConsumer) {
        startCriticalSection(entity);
        try {
            entityConsumer.accept(entity);
        } finally {
            endCriticalSection(entity);
        }
    }

    public static void startCriticalSection(@NonNull Entity entity) {
        startCriticalSection(entity.getId());
    }

    public static void startCriticalSection(@NonNull EntityId id) {
        Lock lock = concurrentHashMap.computeIfAbsent(id, entityId -> {
            log.info("No locks for {} so gonna create one!", entityId);
            return new ReentrantLock();
        });
        log.info("--->>> Gonna lock on {}!", id);
        lock.lock();
        log.info("--->>> Successfully locked on {}!", id);
    }

    public static void endCriticalSection(@NonNull Entity entity) {
        endCriticalSection(entity.getId());
    }

    public static void endCriticalSection(@NonNull EntityId id) {
        Lock lock = concurrentHashMap.get(id);
        if (lock != null) {
            log.info("<<<--- Gonna unlock on {}!", id);
            lock.unlock();
            log.info("<<<--- Successfully unlocked on {}!", id);
        } else {
            throw new IllegalStateException("One can't unlock something that wasn't locked! id=" + id);
        }
    }

    // simple non-production usage

    public static void main(String... args) throws InterruptedException, ExecutionException {
        var entity1 = new DefaultEntity(new DefaultEntityId());
        var entity2 = new DefaultEntity(new DefaultEntityId());

        var callable1 = new Callable<>() {
            @Override
            public Object call() throws Exception {
                startCriticalSection(entity1);
                Thread.sleep(5_000);
                endCriticalSection(entity1);
                return null;
            }
        };

        var callable2 = new Callable<>() {
            @Override
            public Object call() throws Exception {
                startCriticalSection(entity2);
                Thread.sleep(7_000);
                endCriticalSection(entity2);
                return null;
            }
        };

        var callable3 = new Callable<>() {
            @Override
            public Object call() throws Exception {
                exclusivelyDoWithEntity(entity1, entity -> log.info("In accept() with {}", entity));
                return null;
            }
        };

        var callables = List.of(callable1, callable2, callable3);
        ExecutorService es = Executors.newFixedThreadPool(callables.size());
        var futures = es.invokeAll(callables);
        for (var future : futures) {
            future.get();
        }
        es.shutdownNow();
    }

    // default impls

    @Data
    public static class DefaultEntityId implements EntityId {

        private static final AtomicInteger instanceCounter = new AtomicInteger();

        private final Integer value = instanceCounter.incrementAndGet();

    }

    @Data
    @AllArgsConstructor
    public static class DefaultEntity implements Entity {

        private final EntityId entityId;

        @Override
        public EntityId getId() {
            return entityId;
        }
    }
}
