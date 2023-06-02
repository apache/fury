package io.fury.pool;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.fury.Fury;
import io.fury.util.LoaderBinding;
import io.fury.util.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * fury pool factory The pool is used to initialize instances of fury related objects for soft.
 * connections
 *
 * @author yanhuai
 */
public class FuryPooledObjectFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FuryPooledObjectFactory.class);

    private final Function<ClassLoader, Fury> furyFactory;

    /**
     * ClassLoaderFuryPooled cache, have all. ClassLoaderFuryPooled caches key:
     * WeakReference:ClassLoader value: SoftReference:ClassLoaderFuryPooled
     *
     * @see Cache
     * @see com.google.common.cache.CacheBuilder
     */
    private final Cache<ClassLoader, ClassLoaderFuryPooled> classLoaderFuryPooledCache;

    /** ThreadLocal: ClassLoader. */
    private final ThreadLocal<ClassLoader> classLoaderLocal =
            ThreadLocal.withInitial(() -> Thread.currentThread().getContextClassLoader());

    /**
     * Dynamic capacity expansion and contraction The user sets the minimum number of object pools.
     */
    private final int minPoolSize;

    /**
     * Dynamic capacity expansion and contraction The user sets the maximum number of object pools.
     * Math.max(maxPoolSize, CPU * 2)
     */
    private final int maxPoolSize;

    public FuryPooledObjectFactory(
            Function<ClassLoader, Fury> furyFactory,
            int minPoolSize,
            int maxPoolSize,
            long expireTime,
            TimeUnit timeUnit) {
        this.minPoolSize = minPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.furyFactory = furyFactory;
        classLoaderFuryPooledCache =
                CacheBuilder.newBuilder()
                        .weakKeys()
                        .softValues()
                        .expireAfterAccess(expireTime, timeUnit)
                        .build();
    }

    public Fury getFury() {
        try {
            ClassLoader classLoader = classLoaderLocal.get();
            ClassLoaderFuryPooled classLoaderFuryPooled =
                    classLoaderFuryPooledCache.getIfPresent(classLoader);
            if (classLoaderFuryPooled == null) {
                // ifPresent will be cleared when cache expire 30's
                addCache(classLoader);
                return getFury();
            }
            return classLoaderFuryPooled.getFury();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void returnFury(Fury fury) {
        try {
            ClassLoader classLoader = classLoaderLocal.get();
            ClassLoaderFuryPooled classLoaderFuryPooled =
                    classLoaderFuryPooledCache.getIfPresent(classLoader);
            if (classLoaderFuryPooled == null) {
                // ifPresent will be cleared when cache expire 30's
                return;
            }
            classLoaderFuryPooled.returnFury(fury);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /** todo setClassLoader support LoaderBinding.StagingType */
    public void setClassLoader(ClassLoader classLoader, LoaderBinding.StagingType stagingType) {
        classLoaderLocal.set(classLoader);
        addCache(classLoader);
    }

    public ClassLoader getClassLoader() {
        return classLoaderLocal.get();
    }

    public void clearClassLoader(ClassLoader loader) {
        classLoaderFuryPooledCache.invalidate(loader);
        classLoaderLocal.remove();
    }

    private synchronized void addCache(ClassLoader classLoader) {
        ClassLoaderFuryPooled classLoaderFuryPooled =
                classLoaderFuryPooledCache.getIfPresent(classLoader);
        if (classLoaderFuryPooled == null) {
            classLoaderFuryPooled =
                    new ClassLoaderFuryPooled(classLoader, furyFactory, minPoolSize, maxPoolSize);
            classLoaderFuryPooledCache.put(classLoader, classLoaderFuryPooled);
        }
    }
}
