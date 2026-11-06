package com.rpl.agentorama.impl;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.*;

import com.rpl.agentorama.InvalidationCause;
import com.rpl.rama.integration.*;

import clojure.lang.ILookup;
import clojure.lang.Keyword;

public class AgentNodeExecutorTaskGlobal implements TaskGlobalObject {
  private static final Keyword UUID_KW = Keyword.intern(null, "uuid");

  WorkerManagedResource<ExecutorService> _execServResource;
  // At most one attempt per node invoke id. Each entry ends with the attempt's
  // NodeComplete being processed, its body throwing, or its invalidation, and
  // not when its thread returns: until NodeComplete is processed the node record
  // has no finish time, so the stall checker would take the node for stalled.
  // An attempt only ever removes its own entry, so a late finish from an
  // invalidated attempt can't untrack the current one.
  ConcurrentHashMap<UUID, NodeAttempt> _runningInvokeIds;

  // type is opaque here - only passed into clojure
  private Object _throttler;

  private static final ThreadLocal<Object> LOG_THROTTLER = new ThreadLocal<>();

  // attempt is null for work that isn't a node execution; it is not tracked
  public void submitTask(NodeAttempt attempt, clojure.lang.AFn f) {
    Runnable wrappedTask = () -> {
      try {
	      LOG_THROTTLER.set(_throttler);
        f.run();
	      LOG_THROTTLER.remove();
      } catch (Throwable t) {
        if(attempt!=null) removeTrackedInvokeId(attempt.invokeId, attempt.retryNum);
        throw t;
      }
    };
    ExecutorService exec = _execServResource.getResource();
    if(attempt==null) {
      exec.submit(wrappedTask);
    } else {
      // Our own FutureTask rather than the Future from submit: the attempt has
      // it before any invalidation can happen, and cancel(true) interrupts its
      // thread whatever type the executor would have returned.
      FutureTask<Void> task = new FutureTask<>(wrappedTask, null);
      attempt.setTask(task);
      _runningInvokeIds.put(attempt.invokeId, attempt);
      exec.execute(task);
    }
  }

  // Signals and interrupts the run's attempts below belowRetryNum. They are
  // removed here because a task that hasn't started never will, so nothing else
  // would remove them, and an interrupted body appends nothing. Runs on the task
  // thread, so no attempt of the run can be submitted during this.
  public void interruptAttempts(String agentName, long agentTaskId, Object agentId, long belowRetryNum, InvalidationCause cause) {
    for(NodeAttempt a: _runningInvokeIds.values()) {
      if(a.retryNum<belowRetryNum && a.belongsToRun(agentName, agentTaskId, agentId)) {
        a.invalidate(cause);
        removeTrackedInvokeId(a.invokeId, a.retryNum);
      }
    }
  }

  public Set<UUID> getRunningInvokeIds() {
    return new HashSet(_runningInvokeIds.keySet());
  }

  @Override
  public void prepareForTask(int taskId, TaskGlobalContext context) {
    _execServResource = new WorkerManagedResource("agentVirtualThreads", context, () -> Executors.newVirtualThreadPerTaskExecutor());
    _runningInvokeIds = new ConcurrentHashMap();
    _throttler = context.getLogThrottler();
  }

  // Removes the entry only if it tracks the given attempt.
  public void removeTrackedInvokeId(UUID invokeId, long retryNum) {
    _runningInvokeIds.computeIfPresent(invokeId, (id, a) -> a.retryNum==retryNum ? null : a);
  }

  // Removes the entry whatever attempt it tracks.
  public void removeTrackedInvokeId(UUID invokeId) {
    _runningInvokeIds.remove(invokeId);
  }

  // Doesn't replace an entry of another attempt: a stale attempt asking for
  // input must not hide the current attempt's pending request. No entry usually
  // means tracking was reset by a leader change, so the attempt is tracked again
  // to stay reachable, unless it's gone because it was invalidated.
  public void putHumanFuture(NodeAttempt attempt, Object request, CompletableFuture cf) {
    _runningInvokeIds.compute(attempt.invokeId, (id, a) -> {
      boolean track = a==null ? !attempt.isInvalidated() : a.retryNum==attempt.retryNum;
      if(!track) return a;
      attempt.setHumanWait(new NodeAttempt.HumanWait(request, cf));
      return attempt;
    });
  }

  private NodeAttempt.HumanWait getHumanWait(UUID invokeId) {
    NodeAttempt a = _runningInvokeIds.get(invokeId);
    return a==null ? null : a.getHumanWait();
  }

  public CompletableFuture getHumanFuture(UUID invokeId, String uuid) {
    NodeAttempt.HumanWait w = getHumanWait(invokeId);
    if(w!=null) {
      ILookup m = (ILookup) w.request;
      if(m.valAt(UUID_KW).equals(uuid)) {
        return w.future;
      }
    }
    return null;
  }

  public Object getHumanRequest(UUID invokeId) {
    NodeAttempt.HumanWait w = getHumanWait(invokeId);
    return w==null ? null : w.request;
  }

  public static Object getLogThrottler() {
    return LOG_THROTTLER.get();
  }

  @Override
  public void gainedLeadership() {
    _runningInvokeIds = new ConcurrentHashMap();
  }

  @Override
  public void close() throws IOException {
    _execServResource.close();
  }
}
