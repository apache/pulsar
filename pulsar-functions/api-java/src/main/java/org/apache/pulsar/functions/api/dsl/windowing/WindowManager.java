/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.api.dsl.windowing;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.pulsar.functions.api.dsl.windowing.EvictionPolicy.Action.EXPIRE;
import static org.apache.pulsar.functions.api.dsl.windowing.EvictionPolicy.Action.PROCESS;
import static org.apache.pulsar.functions.api.dsl.windowing.EvictionPolicy.Action.STOP;

/**
 * Tracks a window of events and fires {@link WindowLifecycleListener} callbacks
 * on expiry of events or activation of the window due to {@link TriggerPolicy}.
 *
 * @param <T> the type of event in the window.
 */
@Slf4j
public class WindowManager<T> implements TriggerHandler {
  
  /**
   * Expire old events every EXPIRE_EVENTS_THRESHOLD to
   * keep the window size in check.
   * <p>
   * Note that if the eviction policy is based on watermarks, events will not be evicted until a new
   * watermark would cause them to be considered expired anyway, regardless of this limit
   */
  public static final int EXPIRE_EVENTS_THRESHOLD = 100;

  protected final Collection<Event<T>> queue;
  protected EvictionPolicy<T, ?> evictionPolicy;
  protected TriggerPolicy<T, ?> triggerPolicy;
  protected final WindowLifecycleListener<Event<T>> windowLifecycleListener;
  private final List<Event<T>> expiredEvents;
  private final Set<Event<T>> prevWindowEvents;
  private final AtomicInteger eventsSinceLastExpiry;

  /**
   * Constructs a {@link WindowManager}
   *
   * @param lifecycleListener the {@link WindowLifecycleListener}
   * @param queue a collection where the events in the window can be enqueued.
   * <br/>
   * <b>Note:</b> This collection has to be thread safe.
   */
  public WindowManager(WindowLifecycleListener<Event<T>> lifecycleListener, Collection<Event<T>> queue) {
    windowLifecycleListener = lifecycleListener;
    this.queue = queue;
    expiredEvents = new ArrayList<>();
    prevWindowEvents = new HashSet<>();
    eventsSinceLastExpiry = new AtomicInteger();
  }

  public void setEvictionPolicy(EvictionPolicy<T, ?> evictionPolicy) {
    this.evictionPolicy = evictionPolicy;
  }

  public void setTriggerPolicy(TriggerPolicy<T, ?> triggerPolicy) {
    this.triggerPolicy = triggerPolicy;
  }

  /**
   * Add an event into the window, with the given ts as the tracking ts.
   *
   * @param event the event to track
   * @param ts the timestamp
   */
  public void add(T event, long ts, byte[] messageId, String topic) {
    add(new EventImpl<>(event, ts, messageId, topic));
  }

  /**
   * Tracks a window event
   *
   * @param windowEvent the window event to track
   */
  public void add(Event<T> windowEvent) {
    log.info("add windowEvent: {}", windowEvent);
    // watermark events are not added to the queue.
    if (windowEvent.isWatermark()) {
      log.debug(String.format("Got watermark event with ts %d", windowEvent.getTimestamp()));
    } else {
      queue.add(windowEvent);
    }
    track(windowEvent);
    compactWindow();
  }

  /**
   * The callback invoked by the trigger policy.
   */
  @Override
  public boolean onTrigger() {
    List<Event<T>> windowEvents = null;
    List<Event<T>> expired = null;

    /*
     * scan the entire window to handle out of order events in
     * the case of time based windows.
     */
    windowEvents = scanEvents(true);
    expired = new ArrayList<>(expiredEvents);
    expiredEvents.clear();
    List<Event<T>> events = new ArrayList<>();
    List<Event<T>> newEvents = new ArrayList<>();
    for (Event<T> event : windowEvents) {
      events.add(event);
      if (!prevWindowEvents.contains(event)) {
        newEvents.add(event);
      }
    }
    prevWindowEvents.clear();
    if (!events.isEmpty()) {
      prevWindowEvents.addAll(windowEvents);
      log.debug(String.format("invoking windowLifecycleListener onActivation, [%d] events in "
          + "window.", events.size()));
      windowLifecycleListener.onActivation(events, newEvents, expired,
          evictionPolicy.getContext().getReferenceTime());
    } else {
      log.debug("No events in the window, skipping onActivation");
    }
    triggerPolicy.reset();
    return !events.isEmpty();
  }

  public void shutdown() {
    log.debug("Shutting down WindowManager");
    if (triggerPolicy != null) {
      triggerPolicy.shutdown();
    }
  }

  /**
   * expires events that fall out of the window every
   * EXPIRE_EVENTS_THRESHOLD so that the window does not grow
   * too big.
   */
  protected void compactWindow() {
    if (eventsSinceLastExpiry.incrementAndGet() >= EXPIRE_EVENTS_THRESHOLD) {
      scanEvents(false);
    }
  }

  /**
   * feed the event to the eviction and trigger policies
   * for bookkeeping and optionally firing the trigger.
   */
  private void track(Event<T> windowEvent) {
    evictionPolicy.track(windowEvent);
    triggerPolicy.track(windowEvent);
  }

  /**
   * Scan events in the queue, using the expiration policy to check
   * if the event should be evicted or not.
   *
   * @param fullScan if set, will scan the entire queue; if not set, will stop
   * as soon as an event not satisfying the expiration policy is found
   * @return the list of events to be processed as a part of the current window
   */
  private List<Event<T>> scanEvents(boolean fullScan) {
    log.debug(String.format("Scan events, eviction policy %s", evictionPolicy));
    List<Event<T>> eventsToExpire = new ArrayList<>();
    List<Event<T>> eventsToProcess = new ArrayList<>();

    Iterator<Event<T>> it = queue.iterator();
    while (it.hasNext()) {
      Event<T> windowEvent = it.next();
      EvictionPolicy.Action action = evictionPolicy.evict(windowEvent);
      if (action == EXPIRE) {
        eventsToExpire.add(windowEvent);
        it.remove();
      } else if (!fullScan || action == STOP) {
        break;
      } else if (action == PROCESS) {
        eventsToProcess.add(windowEvent);
      }
    }
    expiredEvents.addAll(eventsToExpire);

    eventsSinceLastExpiry.set(0);
    log.debug(String.format("[%d] events expired from window.", eventsToExpire.size()));
    if (!eventsToExpire.isEmpty()) {
      log.debug("invoking windowLifecycleListener.onExpiry");
      windowLifecycleListener.onExpiry(eventsToExpire);
    }
    return eventsToProcess;
  }

  @Override
  public String toString() {
    return "WindowManager{" + "evictionPolicy=" + evictionPolicy + ", triggerPolicy="
        + triggerPolicy + '}';
  }
}
