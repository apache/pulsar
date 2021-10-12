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
package org.apache.pulsar.functions.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.Instance;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.scheduler.IScheduler;

@Slf4j
/**
 * The scheduler manager is used to compute scheduling of function instances
 * Only the leader computes new schedulings and writes assignments to the assignment topic
 * The lifecyle of this class is the following:
 *  1. When worker becomes leader, this class with be initialized
 *  2. When worker loses leadership, this class will be closed which also closes the worker's producer to the assignments topic
 */
public class SchedulerManager implements AutoCloseable {

    private final WorkerConfig workerConfig;
    private final ErrorNotifier errorNotifier;
    private final WorkerStatsManager workerStatsManager;
    private ThreadPoolExecutor executorService;
    private final PulsarClient pulsarClient;

    @Setter
    private FunctionMetaDataManager functionMetaDataManager;

    @Setter
    private LeaderService leaderService;

    @Setter
    private MembershipManager membershipManager;

    @Setter
    private FunctionRuntimeManager functionRuntimeManager;

    private final IScheduler scheduler;

    private Producer<byte[]> exclusiveProducer;

    private ScheduledExecutorService scheduledExecutorService;

    private final PulsarAdmin admin;

    @Getter
    private Lock schedulerLock = new ReentrantLock(true);

    private volatile boolean isRunning = false;

    AtomicBoolean isCompactionNeeded = new AtomicBoolean(false);
    private static final long DEFAULT_ADMIN_API_BACKOFF_SEC = 60;
    public static final String HEARTBEAT_TENANT = "pulsar-function";
    public static final String HEARTBEAT_NAMESPACE = "heartbeat";

    @Getter
    private MessageId lastMessageProduced = null;

    private MessageId metadataTopicLastMessage = MessageId.earliest;

    private AtomicBoolean rebalanceInProgress = new AtomicBoolean(false);
    private Future<?> currentRebalanceFuture;

    private AtomicBoolean drainInProgressFlag = new AtomicBoolean(false);
    private Future<?> currentDrainFuture;
    // The list of assignments moved due to the last drain op on a leader. Used in UTs, and debugging.
    private List<Assignment> assignmentsMovedInLastDrain;

    // Possible status of a drain operation.
    enum DrainOpStatus {
        DrainNotInProgress,
        DrainInProgress,
        DrainCompleted
    };

    // A map to hold the status of recent drain operations.
    // It is of the form {workerId : DrainOpStatus}.
    // Entries are added when a drain operation starts, and removed on a periodic (when the worker is no longer seen
    // on a poll).
    private ConcurrentHashMap<String, DrainOpStatus> drainOpStatusMap = new ConcurrentHashMap<String, DrainOpStatus>();

    public SchedulerManager(WorkerConfig workerConfig,
                            PulsarClient pulsarClient,
                            PulsarAdmin admin,
                            WorkerStatsManager workerStatsManager,
                            ErrorNotifier errorNotifier) {
        this.workerConfig = workerConfig;
        this.pulsarClient = pulsarClient;
        this.admin = admin;
        this.scheduler = Reflections.createInstance(workerConfig.getSchedulerClassName(), IScheduler.class,
                Thread.currentThread().getContextClassLoader());
        this.workerStatsManager = workerStatsManager;
        this.errorNotifier = errorNotifier;
    }

    /**
     * Acquires a exclusive producer.  This method cannot return null.  It can only return a valid exclusive producer
     * or throw NotLeaderAnymore exception.
     *
     * @param isLeader if the worker is still the leader
     * @return A valid exclusive producer
     * @throws WorkerUtils.NotLeaderAnymore if the worker is no longer the leader.
     */
    public Producer<byte[]> acquireExclusiveWrite(Supplier<Boolean> isLeader) throws WorkerUtils.NotLeaderAnymore {
        // creates exclusive producer for assignment topic
        return WorkerUtils.createExclusiveProducerWithRetry(
                pulsarClient,
                workerConfig.getFunctionAssignmentTopic(),
                workerConfig.getWorkerId() + "-scheduler-manager",
                isLeader, 10000);
    }

    public synchronized void initialize(Producer<byte[]> exclusiveProducer) {
        if (!isRunning) {
            log.info("Initializing scheduler manager");
            this.exclusiveProducer = exclusiveProducer;
            executorService = new ThreadPoolExecutor(1, 5, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(5));
            executorService.setThreadFactory(new ThreadFactoryBuilder().setNameFormat("worker-scheduler-%d").build());
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-assignment-topic-compactor"));
            if (workerConfig.getTopicCompactionFrequencySec() > 0) {
                scheduleCompaction(this.scheduledExecutorService, workerConfig.getTopicCompactionFrequencySec());
            }

            isRunning = true;
            lastMessageProduced = null;
        } else {
            log.error("Scheduler Manager entered invalid state");
            errorNotifier.triggerError(new IllegalStateException());
        }
    }

    private Future<?> scheduleInternal(Runnable runnable, String errMsg) {
        if (!leaderService.isLeader()) {
            return CompletableFuture.completedFuture(null);
        }

        try {
            return executorService.submit(() -> {
                schedulerLock.lock();
                try {
                    boolean isLeader = leaderService.isLeader();
                    if (isLeader) {
                        try {
                            runnable.run();
                        } catch (Throwable th) {
                            log.error("Encountered error when invoking scheduler [{}]", errMsg);
                            errorNotifier.triggerError(th);
                        }
                    }
                } finally {
                    schedulerLock.unlock();
                }
            });
        } catch (RejectedExecutionException e) {
            // task queue is full so just ignore
            log.debug("Rejected task to invoke scheduler since task queue is already full");
            return CompletableFuture.completedFuture(null);
        }
    }

    public Future<?> schedule() {
        return scheduleInternal(() -> {
            workerStatsManager.scheduleTotalExecTimeStart();
            invokeScheduler();
            workerStatsManager.scheduleTotalExecTimeEnd();
        }, "Encountered error when invoking scheduler");
    }

    private Future<?> rebalance() {
        return scheduleInternal(() -> {
            workerStatsManager.rebalanceTotalExecTimeStart();
            invokeRebalance();
            workerStatsManager.rebalanceTotalExecTimeEnd();
        }, "Encountered error when invoking rebalance");
    }

    public Future<?> rebalanceIfNotInprogress() {
        if (rebalanceInProgress.compareAndSet(false, true)) {
            val numWorkers = getCurrentAvailableNumWorkers();
            if (numWorkers <= 1) {
                rebalanceInProgress.set(false);
                throw new TooFewWorkersException();
            }
            currentRebalanceFuture = rebalance();
            return currentRebalanceFuture;
        } else {
            throw new RebalanceInProgressException();
        }
    }

    private Future<?> drain(String workerId) {
        return scheduleInternal(() -> {
            workerStatsManager.drainTotalExecTimeStart();
            assignmentsMovedInLastDrain = invokeDrain(workerId);
            workerStatsManager.drainTotalExecTimeEnd();
        }, "Encountered error when invoking drain");
    }

    public Future<?> drainIfNotInProgress(String workerId) {
        if (drainInProgressFlag.compareAndSet(false, true)) {
            try {
                val availableWorkers = getCurrentAvailableWorkers();
                if (availableWorkers.size() <= 1) {
                    throw new TooFewWorkersException();
                }

                // A worker must be specified at this point. This would be set up by the caller.
                Preconditions.checkNotNull(workerId);

                // [We can get stricter, and require that every drain op be followed up with a cleanup of the
                // corresponding worker before any other drain op, so that the drainOpStatusMap should be empty
                // at the next drain operation.]
                if (drainOpStatusMap.containsKey(workerId)) {
                    String warnString = "Worker " + workerId
                            + " was not removed yet from SchedulerManager after previous drain op";
                    log.warn(warnString);
                    throw new WorkerNotRemovedAfterPriorDrainException();
                }

                if (!availableWorkers.contains(workerId)) {
                    log.info("invokeDrain was called for a worker={} which is not currently active", workerId);
                    throw new UnknownWorkerException();
                }

                currentDrainFuture = drain(workerId);
                return currentDrainFuture;
            } finally {
                drainInProgressFlag.set(false);
            }
        } else {
            throw new DrainInProgressException();
        }
    }

    public LongRunningProcessStatus getDrainStatus(String workerId) {
        long startTime = System.nanoTime();
        String errString;
        LongRunningProcessStatus retVal = new LongRunningProcessStatus();
        try {
            val workerStatus = drainOpStatusMap.get(workerId);
            if (workerStatus == null) {
                errString = "Worker " + workerId + " not found in drain records";
                retVal = LongRunningProcessStatus.forError(errString);
            } else {
                switch (workerStatus) {
                    default:
                        errString = "getDrainStatus: Unexpected status " + workerStatus + " found for worker " + workerId;
                        retVal = LongRunningProcessStatus.forError(errString);
                        break;
                    case DrainCompleted:
                        retVal = LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.SUCCESS);
                        break;
                    case DrainInProgress:
                        retVal = LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.RUNNING);
                        break;
                    case DrainNotInProgress:
                        retVal = LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.NOT_RUN);
                        break;
                }
            }
        } finally {
            log.info("Get drain status for worker {} - execution time: {} sec; returning status={}, error={}",
                    workerId, (System.nanoTime() - startTime) / Math.pow(10, 9),
                    retVal.status, retVal.lastError);
            return retVal;
        }
    }

    // The following method is used only for testing.
    @VisibleForTesting
    void clearDrainOpsStatus() {
        drainOpStatusMap.clear();
        log.warn("Cleared drain op status map");
    }

    // The following method is used only for testing.
    @VisibleForTesting
    void setDrainOpsStatus(final String workerId, final DrainOpStatus dStatus) {
        drainOpStatusMap.put(workerId, dStatus);
        log.warn("setDrainOpsStatus: updated drain status of worker {} to {}", workerId, dStatus);
    }

    // The following method is used only for testing.
    @VisibleForTesting
    ConcurrentHashMap<String, DrainOpStatus> getDrainOpsStatusMap() {
        val retVal = new ConcurrentHashMap<String, DrainOpStatus>(drainOpStatusMap);
        return retVal;
    }

    private synchronized int getCurrentAvailableNumWorkers() {
        return getCurrentAvailableWorkers().size();
    }

    private synchronized Set <String> getCurrentAvailableWorkers() {
        Set<String> currentMembership = membershipManager.getCurrentMembership()
                .stream().map(workerInfo -> workerInfo.getWorkerId()).collect(Collectors.toSet());

        // iterate the set, instead of the concurrent hashmap
        Iterator<String> iter = currentMembership.iterator();
        while (iter.hasNext()) {
            if (drainOpStatusMap.containsKey(iter.next())) {
                iter.remove();
            }
        }

        return currentMembership;
    }

    @VisibleForTesting
    void invokeScheduler() {
        long startTime = System.nanoTime();

        Set<String> availableWorkers = getCurrentAvailableWorkers();

        List<FunctionMetaData> allFunctions = functionMetaDataManager.getAllFunctionMetaData();
        Map<String, Function.Instance> allInstances = computeAllInstances(allFunctions, functionRuntimeManager.getRuntimeFactory().externallyManaged());
        Map<String, Map<String, Assignment>> workerIdToAssignments = functionRuntimeManager
                .getCurrentAssignments();

        // initialize stats collection
        SchedulerStats schedulerStats = new SchedulerStats(workerIdToAssignments, availableWorkers);

        //delete assignments of functions and instances that don't exist anymore
        Iterator<Map.Entry<String, Map<String, Assignment>>> it = workerIdToAssignments.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Assignment>> workerIdToAssignmentEntry = it.next();
            Map<String, Assignment> functionMap = workerIdToAssignmentEntry.getValue();

            // remove instances that don't exist anymore
            functionMap.entrySet().removeIf(entry -> {
                String fullyQualifiedInstanceId = entry.getKey();
                boolean deleted = !allInstances.containsKey(fullyQualifiedInstanceId);
                if (deleted) {
                    Assignment assignment = entry.getValue();
                    MessageId messageId = publishNewAssignment(assignment.toBuilder().build(), true);

                    // Directly update in memory assignment cache since I am leader
                    log.info("Deleting assignment: {}", assignment);
                    functionRuntimeManager.deleteAssignment(fullyQualifiedInstanceId);
                    // update message id associated with current view of assignments map
                    lastMessageProduced = messageId;
                    // update stats
                    schedulerStats.removedAssignment(assignment);
                }
                return deleted;
            });

            // update assignment instances in case attributes of a function gets updated
            for (Map.Entry<String, Assignment> entry : functionMap.entrySet()) {
                String fullyQualifiedInstanceId = entry.getKey();
                Assignment assignment = entry.getValue();
                Function.Instance instance = allInstances.get(fullyQualifiedInstanceId);

                if (!assignment.getInstance().equals(instance)) {
                    functionMap.put(fullyQualifiedInstanceId, assignment.toBuilder().setInstance(instance).build());
                    Assignment newAssignment = assignment.toBuilder().setInstance(instance).build().toBuilder().build();
                    MessageId messageId = publishNewAssignment(newAssignment, false);

                    // Directly update in memory assignment cache since I am leader
                    log.info("Updating assignment: {}", newAssignment);
                    functionRuntimeManager.processAssignment(newAssignment);
                    // update message id associated with current view of assignments map
                    lastMessageProduced = messageId;
                    //update stats
                    schedulerStats.updatedAssignment(newAssignment);
                }
                if (functionMap.isEmpty()) {
                    it.remove();
                }
            }
        }

        List<Assignment> currentAssignments = workerIdToAssignments
                .entrySet()
                .stream()
                .filter(workerIdToAssignmentEntry -> {
                    String workerId = workerIdToAssignmentEntry.getKey();
                    // remove assignments to workers that don't exist / died for now.
                    // wait for failure detector to unassign them in the future for re-scheduling
                    if (!availableWorkers.contains(workerId)) {
                        return false;
                    }

                    return true;
                })
                .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream())
                .collect(Collectors.toList());

        Pair<List<Function.Instance>, List<Assignment>> unassignedInstances
                = getUnassignedFunctionInstances(workerIdToAssignments, allInstances);

        workerStatsManager.scheduleStrategyExecTimeStartStart();
        List<Assignment> assignments = scheduler.schedule(unassignedInstances.getLeft(), currentAssignments, availableWorkers);
        workerStatsManager.scheduleStrategyExecTimeStartEnd();

        assignments.addAll(unassignedInstances.getRight());

        if (log.isDebugEnabled()) {
            log.debug("New assignments computed: {}", assignments);
        }

        isCompactionNeeded.set(!assignments.isEmpty());

        for(Assignment assignment : assignments) {
            MessageId messageId = publishNewAssignment(assignment, false);

            // Directly update in memory assignment cache since I am leader
            log.info("Adding assignment: {}", assignment);
            functionRuntimeManager.processAssignment(assignment);
            // update message id associated with current view of assignments map
            lastMessageProduced = messageId;
            // update stats
            schedulerStats.newAssignment(assignment);
        }

        log.info("Schedule summary - execution time: {} sec | total unassigned: {} | stats: {}\n{}",
                (System.nanoTime() - startTime) / Math.pow(10, 9),
                unassignedInstances.getLeft().size(), schedulerStats.getSummary(), schedulerStats);
    }

    private void invokeRebalance() {
        long startTime = System.nanoTime();

        Set<String> availableWorkers = getCurrentAvailableWorkers();

        Map<String, Map<String, Assignment>> workerIdToAssignments = functionRuntimeManager.getCurrentAssignments();

        // initialize stats collection
        SchedulerStats schedulerStats = new SchedulerStats(workerIdToAssignments, availableWorkers);

        // filter out assignments of workers that are not currently in the active membership
        List<Assignment> currentAssignments = workerIdToAssignments
                .entrySet()
                .stream()
                .filter(workerIdToAssignmentEntry -> {
                    String workerId = workerIdToAssignmentEntry.getKey();
                    // remove assignments to workers that don't exist / died for now.
                    // wait for failure detector to unassign them in the future for re-scheduling
                    if (!availableWorkers.contains(workerId)) {
                        return false;
                    }

                    return true;
                })
                .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream())
                .collect(Collectors.toList());

        workerStatsManager.rebalanceStrategyExecTimeStart();
        List<Assignment> rebalancedAssignments = scheduler.rebalance(currentAssignments, availableWorkers);
        workerStatsManager.rebalanceStrategyExecTimeEnd();

        for (Assignment assignment : rebalancedAssignments) {
            MessageId messageId = publishNewAssignment(assignment, false);
            // Directly update in memory assignment cache since I am leader
            log.info("Rebalance - new assignment: {}", assignment);
            functionRuntimeManager.processAssignment(assignment);
            // update message id associated with current view of assignments map
            lastMessageProduced = messageId;
            // update stats
            schedulerStats.newAssignment(assignment);
        }

        log.info("Rebalance summary - execution time: {} sec | stats: {}\n{}",
                (System.nanoTime() - startTime) / Math.pow(10, 9), schedulerStats.getSummary(), schedulerStats);

        rebalanceInProgress.set(false);
    }

    private void scheduleCompaction(ScheduledExecutorService executor, long scheduleFrequencySec) {
        if (executor != null) {
            executor.scheduleWithFixedDelay(() -> {
                if (leaderService.isLeader() && isCompactionNeeded.get()) {
                    compactAssignmentTopic();
                    isCompactionNeeded.set(false);
                }
            }, scheduleFrequencySec, scheduleFrequencySec, TimeUnit.SECONDS);

            executor.scheduleWithFixedDelay(() -> {
                if (leaderService.isLeader() && metadataTopicLastMessage.compareTo(functionMetaDataManager.getLastMessageSeen()) != 0) {
                    metadataTopicLastMessage = functionMetaDataManager.getLastMessageSeen();
                    compactFunctionMetadataTopic();
                }
            }, scheduleFrequencySec, scheduleFrequencySec, TimeUnit.SECONDS);
        }
    }

    // The following method is used only for testing.
    @VisibleForTesting
    List<Assignment> getAssignmentsMovedInLastDrain() {
        return assignmentsMovedInLastDrain;
    }

    // The following method is used only for testing.
    @VisibleForTesting
    void clearAssignmentsMovedInLastDrain() {
        assignmentsMovedInLastDrain = null;
    }

    @VisibleForTesting
    List<Assignment> invokeDrain(String workerId) {

        long startTime = System.nanoTime();

        Set<String> availableWorkers = getCurrentAvailableWorkers();

        // workerIdToAssignments is a map of the form {workerId : {FullyQualifiedInstanceId : Assignment}}
        Map<String, Map<String, Assignment>> workerIdToAssignments = functionRuntimeManager.getCurrentAssignments();

        // initialize stats collection
        SchedulerStats schedulerStats = new SchedulerStats(workerIdToAssignments, availableWorkers);

        boolean drainSuccessful = false;
        List<Assignment> postDrainAssignments = null;

        try {
            drainOpStatusMap.put(workerId, DrainOpStatus.DrainInProgress);

            availableWorkers.remove(workerId);

            List<FunctionMetaData> allFunctions = functionMetaDataManager.getAllFunctionMetaData();
            Map<String, Function.Instance> allInstances =
                    computeAllInstances(allFunctions, functionRuntimeManager.getRuntimeFactory().externallyManaged());

            // The assignments that were not on the worker being drained don't need to change.
            val activeWorkersAssignmentsMap = new HashMap<String, Map<String, Assignment>>();
            List<Assignment> assignmentsOnActiveWorkers = workerIdToAssignments
                    .entrySet()
                    .stream()
                    .filter(workerIdToAssignmentEntry -> {
                        if (workerIdToAssignmentEntry.getKey().compareTo(workerId) != 0) {
                            activeWorkersAssignmentsMap.put(workerIdToAssignmentEntry.getKey(),
                                    workerIdToAssignmentEntry.getValue());
                            return true;
                        }

                        return false;
                    })
                    .flatMap(stringMapEntry -> stringMapEntry.getValue().values().stream())
                    .collect(Collectors.toList());

            Pair<List<Function.Instance>, List<Assignment>> instancesToAssign
                    = getUnassignedFunctionInstances(activeWorkersAssignmentsMap, allInstances);

            workerStatsManager.drainTotalExecTimeStart();
            // Try to schedule the instances on the workers remaining available after "workerId" is removed.
            try {
                postDrainAssignments = scheduler.schedule(instancesToAssign.getLeft(), assignmentsOnActiveWorkers, availableWorkers);
            } catch (Exception e) {
                log.info("invokeDrain: Got exception from schedule: ", e);
            }
            workerStatsManager.drainTotalExecTimeEnd();

            if (postDrainAssignments != null) {
                for (Assignment assignment : postDrainAssignments) {
                    MessageId messageId = publishNewAssignment(assignment, false);
                    // Directly update in memory assignment cache since I am leader
                    functionRuntimeManager.processAssignment(assignment);
                    // update message id associated with current view of assignments map
                    lastMessageProduced = messageId;
                    // update stats
                    schedulerStats.newAssignment(assignment);
                }
            }
            drainOpStatusMap.put(workerId, DrainOpStatus.DrainCompleted);
            drainSuccessful = true;
        } finally {
            log.info("Draining worker {} was {}successful; summary [] - execution time: {} sec | stats: {}\n{}",
                    workerId, drainSuccessful ? "" : "un",
                    (System.nanoTime() - startTime) / Math.pow(10, 9),
                    schedulerStats.getSummary(), schedulerStats);
            return postDrainAssignments;
        }
    }

    private void compactAssignmentTopic() {
        if (this.admin != null) {
            try {
                this.admin.topics().triggerCompaction(workerConfig.getFunctionAssignmentTopic());
            } catch (PulsarAdminException e) {
                log.error("Failed to trigger compaction", e);
                scheduledExecutorService.schedule(() -> compactAssignmentTopic(), DEFAULT_ADMIN_API_BACKOFF_SEC,
                        TimeUnit.SECONDS);
            }
        }
    }

    protected synchronized int updateWorkerDrainMap() {
        long startTime = System.nanoTime();
        int numRemovedWorkerIds = 0;

        if (drainOpStatusMap.size() > 0) {
            val currentMembership = membershipManager.getCurrentMembership()
                    .stream().map(workerInfo -> workerInfo.getWorkerId()).collect(Collectors.toSet());
            val removeWorkerIds = new ArrayList<String>();

            for (String workerId : drainOpStatusMap.keySet()) {
                if (!currentMembership.contains(workerId)) {
                    removeWorkerIds.add(workerId);
                }
            }
            for (String workerId : removeWorkerIds) {
                drainOpStatusMap.remove(workerId);
            }
            numRemovedWorkerIds = removeWorkerIds.size();
        }

        if (numRemovedWorkerIds > 0) {
            log.info("cleanupWorkerDrainMap removed {} stale workerIds in {} sec",
                numRemovedWorkerIds, (System.nanoTime() - startTime) / Math.pow(10, 9));
        }

        return numRemovedWorkerIds;
    }

    private void compactFunctionMetadataTopic() {
        if (this.admin != null) {
            try {
                this.admin.topics().triggerCompaction(workerConfig.getFunctionMetadataTopic());
            } catch (PulsarAdminException e) {
                log.error("Failed to trigger compaction", e);
                scheduledExecutorService.schedule(() -> compactFunctionMetadataTopic(), DEFAULT_ADMIN_API_BACKOFF_SEC,
                        TimeUnit.SECONDS);
            }
        }
    }

    private MessageId publishNewAssignment(Assignment assignment, boolean deleted) {
        try {
            String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
            // publish empty message with instance-id key so, compactor can delete and skip delivery of this instance-id
            // message
            return exclusiveProducer.newMessage().key(fullyQualifiedInstanceId)
                    .value(deleted ? "".getBytes() : assignment.toByteArray()).send();
        } catch (Exception e) {
            log.error("Failed to {} assignment update {}", assignment, deleted ? "send" : "deleted", e);
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Function.Instance> computeAllInstances(List<FunctionMetaData> allFunctions,
                                                                     boolean externallyManagedRuntime) {
        Map<String, Function.Instance> functionInstances = new HashMap<>();
        for (FunctionMetaData functionMetaData : allFunctions) {
            for (Function.Instance instance : computeInstances(functionMetaData, externallyManagedRuntime)) {
                functionInstances.put(FunctionCommon.getFullyQualifiedInstanceId(instance), instance);
            }
        }
        return functionInstances;
    }

    static List<Function.Instance> computeInstances(FunctionMetaData functionMetaData,
                                                           boolean externallyManagedRuntime) {
        List<Function.Instance> functionInstances = new LinkedList<>();
        if (!externallyManagedRuntime) {
            int instances = functionMetaData.getFunctionDetails().getParallelism();
            for (int i = 0; i < instances; i++) {
                functionInstances.add(Function.Instance.newBuilder()
                        .setFunctionMetaData(functionMetaData)
                        .setInstanceId(i)
                        .build());
            }
        } else {
            functionInstances.add(Function.Instance.newBuilder()
                    .setFunctionMetaData(functionMetaData)
                    .setInstanceId(-1)
                    .build());
        }
        return functionInstances;
    }

    private Pair<List<Function.Instance>, List<Assignment>> getUnassignedFunctionInstances(
            Map<String, Map<String, Assignment>> currentAssignments, Map<String, Function.Instance> functionInstances) {

        List<Function.Instance> unassignedFunctionInstances = new LinkedList<>();
        List<Assignment> heartBeatAssignments = Lists.newArrayList();
        Map<String, Assignment> assignmentMap = new HashMap<>();
        if (currentAssignments != null) {
            for (Map<String, Assignment> entry : currentAssignments.values()) {
                assignmentMap.putAll(entry);
            }
        }

        for (Map.Entry<String, Function.Instance> instanceEntry : functionInstances.entrySet()) {
            String fullyQualifiedInstanceId = instanceEntry.getKey();
            Function.Instance instance = instanceEntry.getValue();
            String heartBeatWorkerId = checkHeartBeatFunction(instance);
            if (heartBeatWorkerId != null) {
                heartBeatAssignments
                        .add(Assignment.newBuilder().setInstance(instance).setWorkerId(heartBeatWorkerId).build());
                continue;
            }
            if (!assignmentMap.containsKey(fullyQualifiedInstanceId)) {
                unassignedFunctionInstances.add(instance);
            }
        }
        return ImmutablePair.of(unassignedFunctionInstances, heartBeatAssignments);
    }

    @Override
    public synchronized void close() {
        log.info("Closing scheduler manager");
        // make sure we are not closing while a scheduling is being calculated
        schedulerLock.lock();
        try {
            isRunning = false;

            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
            }

            if (executorService != null) {
                executorService.shutdown();
            }

            if (exclusiveProducer != null) {
                try {
                    exclusiveProducer.close();
                } catch (PulsarClientException e) {
                    log.warn("Failed to shutdown scheduler manager assignment producer", e);
                }
            }
        } finally {
            schedulerLock.unlock();
        }
    }

    static String checkHeartBeatFunction(Instance funInstance) {
        if (funInstance.getFunctionMetaData() != null
                && funInstance.getFunctionMetaData().getFunctionDetails() != null) {
            FunctionDetails funDetails = funInstance.getFunctionMetaData().getFunctionDetails();
            return HEARTBEAT_TENANT.equals(funDetails.getTenant())
                    && HEARTBEAT_NAMESPACE.equals(funDetails.getNamespace()) ? funDetails.getName() : null;
        }
        return null;
    }

    public static class RebalanceInProgressException extends RuntimeException {
    }

    public static class DrainInProgressException extends RuntimeException {
    }

    public static class TooFewWorkersException extends RuntimeException {
    }

    public static class UnknownWorkerException extends RuntimeException {
    }

    public static class WorkerNotRemovedAfterPriorDrainException extends RuntimeException {
    }

    private static class SchedulerStats {
        @Builder
        @Data
        private static class WorkerStats {
            private int originalNumAssignments;
            private int finalNumAssignments;
            private int instancesAdded;
            private int instancesRemoved;
            private int instancesUpdated;
            private boolean alive;
        }

        private Map<String, WorkerStats> workerStatsMap = new HashMap<>();

        private Map<String, String> instanceToWorkerId = new HashMap<>();

        public SchedulerStats(Map<String, Map<String, Assignment>> workerIdToAssignments, Set<String> workers) {

            for(String workerId : workers) {
                WorkerStats.WorkerStatsBuilder workerStats = WorkerStats.builder().alive(true);
                Map<String, Assignment> assignmentMap = workerIdToAssignments.get(workerId);
                if (assignmentMap != null) {
                    workerStats.originalNumAssignments(assignmentMap.size());
                    workerStats.finalNumAssignments(assignmentMap.size());

                    for (String fullyQualifiedInstanceId : assignmentMap.keySet()) {
                        instanceToWorkerId.put(fullyQualifiedInstanceId, workerId);
                    }
                } else {
                    workerStats.originalNumAssignments(0);
                    workerStats.finalNumAssignments(0);
                }

                workerStatsMap.put(workerId, workerStats.build());
            }

            // workers with assignments that are dead
            for (Map.Entry<String, Map<String, Assignment>> entry : workerIdToAssignments.entrySet()) {
                String workerId = entry.getKey();
                Map<String, Assignment> assignmentMap = entry.getValue();
                if (!workers.contains(workerId)) {
                    WorkerStats workerStats = WorkerStats.builder()
                            .alive(false)
                            .originalNumAssignments(assignmentMap.size())
                            .finalNumAssignments(assignmentMap.size())
                            .build();
                    workerStatsMap.put(workerId, workerStats);
                }
            }
        }

        public void removedAssignment(Assignment assignment) {
            String workerId = assignment.getWorkerId();
            WorkerStats stats = workerStatsMap.get(workerId);
            Preconditions.checkNotNull(stats);

            stats.instancesRemoved++;
            stats.finalNumAssignments--;
        }

        public void newAssignment(Assignment assignment) {
            String fullyQualifiedInstanceId = FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance());
            String newWorkerId = assignment.getWorkerId();
            String oldWorkerId = instanceToWorkerId.get(fullyQualifiedInstanceId);
            if (oldWorkerId != null) {
                WorkerStats oldWorkerStats = workerStatsMap.get(oldWorkerId);
                Preconditions.checkNotNull(oldWorkerStats);

                oldWorkerStats.instancesRemoved++;
                oldWorkerStats.finalNumAssignments--;
            }

            WorkerStats newWorkerStats = workerStatsMap.get(newWorkerId);
            Preconditions.checkNotNull(newWorkerStats);

            newWorkerStats.instancesAdded++;
            newWorkerStats.finalNumAssignments++;
        }

        public void updatedAssignment(Assignment assignment) {
            String workerId = assignment.getWorkerId();
            WorkerStats stats = workerStatsMap.get(workerId);
            Preconditions.checkNotNull(stats);

            stats.instancesUpdated++;
        }

        public String getSummary() {
            int totalAdded = 0;
            int totalUpdated = 0;
            int totalRemoved = 0;

            for (Map.Entry<String, WorkerStats> entry : workerStatsMap.entrySet()) {
                WorkerStats workerStats = entry.getValue();
                totalAdded += workerStats.instancesAdded;
                totalUpdated += workerStats.instancesUpdated;
                totalRemoved += workerStats.instancesRemoved;
            }

            return String.format("{\"Added\": %d, \"Updated\": %d, \"removed\": %d}", totalAdded, totalUpdated, totalRemoved);
        }

        @Override
        public String toString() {
            try {
                return ObjectMapperFactory.getThreadLocal().writerWithDefaultPrettyPrinter().writeValueAsString(workerStatsMap);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
