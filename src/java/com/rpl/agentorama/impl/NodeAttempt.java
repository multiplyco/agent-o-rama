package com.rpl.agentorama.impl;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import com.rpl.agentorama.InvalidationCause;

// One execution of a node. The node invoke id is reused across retry attempts
// (a :continue retry re-runs an incomplete node under the same id), so an
// execution reaches its own attempt through this object, never by looking up
// the invoke id, which could find the next attempt.
public final class NodeAttempt {
  final String agentName;
  final long agentTaskId;
  final Object agentId;
  final UUID invokeId;
  final long retryNum;

  private final AtomicReference<InvalidationCause> _cause = new AtomicReference<>();
  private final Queue<Runnable> _callbacks = new ConcurrentLinkedQueue<>();
  private volatile Future<?> _task;
  private volatile HumanWait _humanWait;

  static final class HumanWait {
    final Object request;
    final CompletableFuture future;

    HumanWait(Object request, CompletableFuture future) {
      this.request = request;
      this.future = future;
    }
  }

  public NodeAttempt(String agentName, long agentTaskId, Object agentId, UUID invokeId, long retryNum) {
    this.agentName = agentName;
    this.agentTaskId = agentTaskId;
    this.agentId = agentId;
    this.invokeId = invokeId;
    this.retryNum = retryNum;
  }

  public boolean isInvalidated() {
    return _cause.get()!=null;
  }

  public InvalidationCause getInvalidationCause() {
    return _cause.get();
  }

  // Runs the callback once, on its own virtual thread: invalidation happens on
  // a task thread, which user code must not block.
  public void onInvalidated(Runnable callback) {
    _callbacks.add(callback);
    if(isInvalidated()) runCallbacks();
  }

  private void runCallbacks() {
    Runnable callback;
    while((callback = _callbacks.poll())!=null) {
      Thread.startVirtualThread(callback);
    }
  }

  // The first cause stays. The signal comes before the interrupt, so that code
  // reacting to the interrupt can tell it came from here.
  void invalidate(InvalidationCause cause) {
    _cause.compareAndSet(null, cause);
    runCallbacks();
    Future<?> task = _task;
    if(task!=null) task.cancel(true);
  }

  boolean belongsToRun(String agentName, long agentTaskId, Object agentId) {
    return this.agentTaskId==agentTaskId && this.agentName.equals(agentName) && this.agentId.equals(agentId);
  }

  void setTask(Future<?> task) {
    _task = task;
  }

  HumanWait getHumanWait() {
    return _humanWait;
  }

  void setHumanWait(HumanWait humanWait) {
    _humanWait = humanWait;
  }
}
