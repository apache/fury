package io.fury.pool;

import io.fury.Fury;
import io.fury.util.LoggerFactory;
import org.slf4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/** for every classloader, have a pool whit fury instance. */
public class ClassLoaderFuryPooled {

    private static final Logger logger = LoggerFactory.getLogger(ClassLoaderFuryPooled.class);

    private final Function<ClassLoader, Fury> furyFactory;

    private final ClassLoader classLoader;

    /**
     * idle Fury cache change. by : 1. getLoaderBind() 2. returnObject(LoaderBinding) 3.
     * addObjAndWarp()
     */
    private final Queue<Fury> idleCacheQueue;

    /** active cache size's number change by : 1. getLoaderBind() 2. returnObject(LoaderBinding). */
    private final AtomicInteger activeCacheNumber = new AtomicInteger(0);

    /**
     * Dynamic capacity expansion and contraction The user sets the maximum number of object pools.
     * Math.max(maxPoolSize, CPU * 2)
     */
    private final int maxPoolSize;

    private final Lock lock = new ReentrantLock();
    private final Condition furyCondition = lock.newCondition();

    public ClassLoaderFuryPooled(
            ClassLoader classLoader,
            Function<ClassLoader, Fury> furyFactory,
            int minPoolSize,
            int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        this.furyFactory = furyFactory;
        this.classLoader = classLoader;
        idleCacheQueue = new ConcurrentLinkedQueue<>();
        while (idleCacheQueue.size() < minPoolSize) {
            addFury();
        }
    }

    public Fury getFury() {
        try {
            lock.lock();
            Fury fury = idleCacheQueue.poll();
            while (fury == null) {
                if (activeCacheNumber.get() < maxPoolSize) {
                    addFury();
                } else {
                    furyCondition.await();
                }
                fury = idleCacheQueue.poll();
                if (fury == null) {
                    continue;
                }
                break;
            }
            activeCacheNumber.incrementAndGet();
            return fury;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void returnFury(Fury fury) {
        try {
            lock.lock();
            idleCacheQueue.add(fury);
            activeCacheNumber.decrementAndGet();
            furyCondition.signalAll();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    private void addFury() {
        Fury fury = furyFactory.apply(classLoader);
        idleCacheQueue.add(fury);
    }
}
