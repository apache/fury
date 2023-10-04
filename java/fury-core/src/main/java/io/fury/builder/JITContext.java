/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.builder;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.fury.Fury;
import io.fury.annotation.Internal;
import io.fury.codegen.CodeGenerator;
import io.fury.config.Config;
import io.fury.util.Platform;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * A context for managing jit serialization code generation in async multithreaded environment.
 *
 * @author chaokunyang
 */
public class JITContext {
  private final Fury fury;
  private final boolean asyncCompilationEnabled;
  // ReentrantLock used Atomic internally first and used thread queue if failed.
  // So it's unnecessary to build atomic locks by fury again.
  private final ReentrantLock jitLock;
  // state for recursive jit fury visit.
  private int furyVisitState;
  private int numRunningTask;
  private final Map<Object, List<NotifyCallback>> hasJITResult;

  public JITContext(Fury fury) {
    this.fury = fury;
    asyncCompilationEnabled = fury.getConfig().isAsyncCompilationEnabled();
    // FIXME(chaokunyang) use fair lock to avoid starving jit thread.
    // It's ok the cost for fail lock is slightly higher than no-fair lock.
    jitLock = new ReentrantLock(true);
    hasJITResult = new HashMap<>();
  }

  /**
   * Register a jit callback which will be invoked if jit is enabled and finished. If jit is
   * disabled, <code>interpreterModeSerializerClass</code> will be returned directly. If async
   * compilation is disabled, <code>jitAction</code> will be invoked directly and return its result,
   *
   * @param interpreterModeAction serializer class without jit.
   * @param jitAction a jit task which will be submitted to the jit thread pool. Note jit task
   *     execution is not protected by a lock for concurrency, thread safety should be done by
   *     itself.
   * @param callback will be invoked when jit finished. Note that this callback is protected by a
   *     lock and unnecessary to be thread safe.
   * @see Config#isAsyncCompilationEnabled()
   */
  @Internal
  public <T> T registerSerializerJITCallback(
      Callable<T> interpreterModeAction, Callable<T> jitAction, SerializerJITCallback<T> callback) {
    try {
      lock();
      if (fury.getConfig().isCodeGenEnabled()) {
        // add `isAsyncVisitingFury()` check so nested object field serializers will
        // be jit-serializers too. Otherwise, we need to switch these field serializers
        // in jit-serializers, which is tricking.
        // TODO(chaokunyang) Submit nested object field serializers jit task to executor,
        //  and update serializer field when jit finished.
        if (fury.getConfig().isAsyncCompilationEnabled() && !isAsyncVisitingFury()) {
          // TODO(chaokunyang) stash callbacks and submit jit task if the serialization speed
          // is really needed.
          ListeningExecutorService compilationService = CodeGenerator.getCompilationService();
          ListenableFuture<T> future;
          hasJITResult.put(callback.id(), new ArrayList<>());
          numRunningTask++;
          future = compilationService.submit(jitAction);
          Futures.addCallback(
              future,
              new FutureCallback<T>() {
                @Override
                public void onSuccess(T result) {
                  try {
                    lock();
                    callback.onSuccess(result);
                    for (NotifyCallback notifyCallback : hasJITResult.get(callback.id())) {
                      notifyCallback.onNotifyResult(result);
                    }
                  } finally {
                    numRunningTask--;
                    if (numRunningTask == 0) {
                      hasJITResult.clear();
                    }
                    unlock();
                  }
                }

                @Override
                public void onFailure(Throwable t) {
                  try {
                    lock();
                    callback.onFailure(t);
                    // ignore onNotifyResult in failed case.
                  } finally {
                    numRunningTask--;
                    if (numRunningTask == 0) {
                      hasJITResult.clear();
                    }
                    unlock();
                  }
                }
              },
              compilationService);
          return interpreterModeAction.call();
        } else {
          return jitAction.call();
        }
      } else {
        return interpreterModeAction.call();
      }
    } catch (Exception e) {
      Platform.throwException(e);
      throw new IllegalStateException("unreachable");
    } finally {
      unlock();
    }
  }

  /** Subscribe jit notify callback to be invoked after target jit finished. */
  public void registerJITNotifyCallback(Object id, NotifyCallback notifyCallback) {
    Preconditions.checkNotNull(id);
    try {
      lock();
      List<NotifyCallback> notifyCallbacks = hasJITResult.get(id);
      if (notifyCallbacks == null) {
        notifyCallback.onNotifyMissed();
      } else {
        notifyCallbacks.add(notifyCallback);
      }
    } finally {
      unlock();
    }
  }

  /**
   * When jit serializers invoke fury related non-thread-safe methods from compiler thread, invoke
   * those methods by this wrapper to get thread safety and memory visibility.
   */
  @Internal
  public <T> T asyncVisitFury(Function<Fury, T> function) {
    try {
      lock();
      furyVisitState++;
      return function.apply(fury);
    } finally {
      furyVisitState--;
      unlock();
    }
  }

  private boolean isAsyncVisitingFury() {
    if (asyncCompilationEnabled) {
      try {
        lock();
        return furyVisitState != 0;
      } finally {
        unlock();
      }
    } else {
      return false;
    }
  }

  public boolean hasJITResult(Object key) {
    try {
      lock();
      return hasJITResult.get(key) != null;
    } finally {
      unlock();
    }
  }

  @Internal
  public void lock() {
    if (asyncCompilationEnabled) {
      jitLock.lock();
    }
  }

  @Internal
  public boolean lockedByCurrentThread() {
    return !asyncCompilationEnabled || jitLock.isHeldByCurrentThread();
  }

  @Internal
  public void unlock() {
    if (asyncCompilationEnabled) {
      jitLock.unlock();
    }
  }

  @Internal
  public interface SerializerJITCallback<T> {
    void onSuccess(T result);

    default void onFailure(Throwable e) {
      e.printStackTrace();
      Platform.throwException(e);
    }

    /** Callback id used to build mapping between jit result, call site and notify site. */
    default Object id() {
      return null;
    }
  }

  @Internal
  public interface NotifyCallback {
    default void onNotifyResult(Object result) {
      onNotifyMissed();
    }

    /**
     * {@link SerializerJITCallback} associated with `id` already finished, related jit states has
     * been cleared.
     */
    void onNotifyMissed();
  }
}
