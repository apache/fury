/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fory.util.concurrency;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

// Mostly derived from Guava 32.1.2
// com.google.common.util.concurrent.MoreExecutors.DirectExecutorService
// https://github.com/google/guava/blob/9f6a3840/guava/src/com/google/common/util/concurrent/MoreExecutors.java
public class DirectExecutorService extends AbstractExecutorService {
  private final Object lock = new Object();
  private int runningTasks = 0;
  private boolean shutdown = false;

  @Override
  public void execute(Runnable command) {
    synchronized (lock) {
      if (shutdown) {
        throw new RejectedExecutionException("Executor already shutdown");
      }
      runningTasks++;
    }
    try {
      command.run();
    } finally {
      synchronized (lock) {
        int numRunning = --runningTasks;
        if (numRunning == 0) {
          lock.notifyAll();
        }
      }
    }
  }

  @Override
  public boolean isShutdown() {
    synchronized (lock) {
      return shutdown;
    }
  }

  @Override
  public void shutdown() {
    synchronized (lock) {
      shutdown = true;
      if (runningTasks == 0) {
        lock.notifyAll();
      }
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown();
    return Collections.emptyList();
  }

  @Override
  public boolean isTerminated() {
    synchronized (lock) {
      return shutdown && runningTasks == 0;
    }
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    synchronized (lock) {
      while (true) {
        if (shutdown && runningTasks == 0) {
          return true;
        } else if (nanos <= 0) {
          return false;
        } else {
          long now = System.nanoTime();
          TimeUnit.NANOSECONDS.timedWait(lock, nanos);
          nanos -= System.nanoTime() - now; // subtract the actual time we waited
        }
      }
    }
  }
}
