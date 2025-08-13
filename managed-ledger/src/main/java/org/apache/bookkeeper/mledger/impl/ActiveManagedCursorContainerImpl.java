/*
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
package org.apache.bookkeeper.mledger.impl;

import static java.util.Objects.requireNonNull;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.StampedLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Implementation of {@link ActiveManagedCursorContainer} that tracks active cursors for cache eviction purposes.
 * This implementation is optimized for the use with cacheEvictionByExpectedReadCount. It doesn't implement
 * the {@link #cursorUpdated(ManagedCursor, Position)} method, as it is not needed for this use case. Cursors
 * are updated using the {@link #updateCursor(ManagedCursor, Position)} method instead. This allows lazy updates
 * to track the ordering of cursors and their positions without the need to immediately reorder the list of cursors
 * on every cursor update. The cacheEvictionByExpectedReadCount use case will only need to know the number of cursors
 * and to be able to know how many cursors are at the same position or before a given position when a backlogged read
 * is performed. When cursors are all performing tailing reads, the ordering of cursors is not important and can be
 * updated lazily when needed.
 */
@Slf4j
public class ActiveManagedCursorContainerImpl implements ActiveManagedCursorContainer {
    private static class Node {
        final ManagedCursor cursor;
        Position position;
        Position pendingPosition;
        boolean pendingRemove = false;
        MutableInt numberOfCursorsAtSamePositionOrBefore;
        Node prev;
        Node next;

        Node(ManagedCursor cursor, Position pendingPosition) {
            this.cursor = cursor;
            this.pendingPosition = pendingPosition;
        }
    }
    int pendingPositionCount = 0;
    // number of nodes in the double-linked list
    int trackedNodeCount = 0;

    public ActiveManagedCursorContainerImpl() {}

    // Head of the double-linked list (sorted by position)
    private Node head = null;
    private Node tail = null;

    // Maps a cursor to the node
    private final ConcurrentMap<String, Node> cursors = new ConcurrentHashMap<>();
    private final Map<String, Node> pendingRemovedCursors = new HashMap<>();

    private final StampedLock rwLock = new StampedLock();

    @Override
    public void add(ManagedCursor cursor, Position position) {
        long stamp = rwLock.writeLock();
        try {
            Node node = cursors.get(cursor.getName());
            if (node != null) {
                if (position == null) {
                    if (node.position != null) {
                        pendingRemovedCursors.put(cursor.getName(), node);
                        node.pendingRemove = true;
                    }
                    if (node.pendingPosition != null) {
                        pendingPositionCount--;
                    }
                    node.pendingPosition = null;
                } else {
                    if (node.pendingPosition == null) {
                        pendingPositionCount++;
                    }
                    node.pendingPosition = position;
                }
            } else {
                if (position != null) {
                    node = pendingRemovedCursors.remove(cursor.getName());
                    if (node != null) {
                        node.pendingRemove = false;
                    }
                }
                if (node == null) {
                    node = new Node(cursor, position);
                }
                if (position != null) {
                    pendingPositionCount++;
                    node.pendingPosition = position;
                }
                cursors.put(cursor.getName(), node);
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    /**
     * Insert a node into the sorted double-linked list and update counters.
     * Nodes with the same position are kept in their insertion order.
     */
    private void insertNodeIntoList(Node node) {
        trackedNodeCount++;

        if (head == null) {
            // First node in the list
            head = tail = node;
            node.numberOfCursorsAtSamePositionOrBefore = new MutableInt(1);
            return;
        }

        // Find the correct position using insertion sort
        Node current = head;
        int countBefore = 0;

        Node firstNodeWithSamePosition = null;

        // Count all nodes with position less than or equal to the new node's position
        // We insert after all nodes with the same position to maintain insertion order
        while (current != null) {
            int comparison = current.position.compareTo(node.position);
            if (comparison <= 0) {
                if (comparison == 0 && firstNodeWithSamePosition == null) {
                    firstNodeWithSamePosition = current;
                }
                if (comparison < 0) {
                    countBefore++;
                }
                current = current.next;
            } else {
                break;
            }
        }

        // Set the counter for the new node
        node.numberOfCursorsAtSamePositionOrBefore =
                firstNodeWithSamePosition != null ? firstNodeWithSamePosition.numberOfCursorsAtSamePositionOrBefore :
                        new MutableInt(countBefore + 1);

        // Insert the node at the correct position
        if (current == null) {
            // Insert at the end
            node.prev = tail;
            tail.next = node;
            tail = node;
        } else if (current == head) {
            // Insert at the beginning
            node.next = head;
            head.prev = node;
            head = node;
        } else {
            // Insert in the middle
            node.prev = current.prev;
            node.next = current;
            current.prev.next = node;
            current.prev = node;
        }

        Node counterUpdateStartNode;
        if (firstNodeWithSamePosition != null) {
            // If we found a node with the same position, we start updating counters from it
            counterUpdateStartNode = firstNodeWithSamePosition;
        } else {
            // Otherwise, we start updating from the next node
            counterUpdateStartNode = node.next;
        }
        // Update counters for nodes
        updateCounters(counterUpdateStartNode, true);
    }

    /**
     * Update counters after inserting or removing a node.
     */
    private void updateCounters(Node startNode, boolean increment) {
        if (startNode == null) {
            return;
        }
        Node current = startNode;
        Position lastUpdatedPosition = null;
        while (current != null) {
            if (lastUpdatedPosition == null || current.position.compareTo(lastUpdatedPosition) != 0) {
                lastUpdatedPosition = current.position;
                MutableInt numberOfCursorsAtSamePositionOrBefore = current.numberOfCursorsAtSamePositionOrBefore;
                if (increment) {
                    numberOfCursorsAtSamePositionOrBefore.increment();
                } else {
                    numberOfCursorsAtSamePositionOrBefore.decrement();
                }
            }
            current = current.next;
        }
    }

    @Override
    public ManagedCursor get(String name) {
        long stamp = rwLock.readLock();
        try {
            Node node = cursors.get(name);
            return node != null ? node.cursor : null;
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    @Override
    public boolean removeCursor(String name) {
        long stamp = rwLock.writeLock();
        try {
            Node node = cursors.remove(name);
            if (node != null) {
                if (node.position != null) {
                    pendingRemovedCursors.put(name, node);
                    node.pendingRemove = true;
                }
                if (node.pendingPosition != null) {
                    pendingPositionCount--;
                    node.pendingPosition = null;
                }
                return true;
            } else {
                return false;
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    /**
     * Remove a node from the double-linked list and update counters.
     */
    private void removeNodeFromList(Node node) {
        Node firstNodeWithSamePositionBeforeNode = findFirstNodeWithSamePositionBeforeNode(node);

        // Remove the node from the list
        if (node.prev != null) {
            node.prev.next = node.next;
        } else {
            // Node was the head
            head = node.next;
        }

        if (node.next != null) {
            node.next.prev = node.prev;
        } else {
            // Node was the tail
            tail = node.prev;
        }

        // Update counters
        updateCounters(
                firstNodeWithSamePositionBeforeNode != null ? firstNodeWithSamePositionBeforeNode : node.next, false);

        // Clear the node's links
        node.prev = null;
        node.next = null;
        trackedNodeCount--;
    }

    private static Node findFirstNodeWithSamePositionBeforeNode(Node node) {
        Position samePosition = node.position;
        // Find the first node with the same position by scanning backwards
        Node firstNodeWithSamePositionBeforeNode = null;
        Node current = node.prev;
        while (current != null && current.position.compareTo(samePosition) == 0) {
            firstNodeWithSamePositionBeforeNode = current;
            current = current.prev;
        }
        return firstNodeWithSamePositionBeforeNode;
    }

    @Override
    public void updateCursor(ManagedCursor cursor, Position newPosition) {
        requireNonNull(cursor);

        long stamp = rwLock.writeLock();
        try {
            Node node = cursors.get(cursor.getName());
            if (node == null) {
                return;
            }
            if (newPosition == null) {
                pendingRemovedCursors.put(cursor.getName(), node);
                node.pendingRemove = true;
                if (node.pendingPosition != null) {
                    pendingPositionCount--;
                }
                node.pendingPosition = null;
            } else {
                if (node.pendingRemove) {
                    node.pendingRemove = false;
                    pendingRemovedCursors.remove(cursor.getName());
                }
                // position changed, mark the node as pending
                if (node.pendingPosition == null) {
                    pendingPositionCount++;
                }
                node.pendingPosition = newPosition;
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    private void processPendingPositions() {
        if (pendingRemovedCursors.isEmpty() && pendingPositionCount == 0) {
            // No pending changes, nothing to do
            return;
        }
        if (pendingPositionCount >= trackedNodeCount) {
            rebuildEntireList();
        } else {
            performIncrementalUpdate();
        }
        if (pendingPositionCount != 0) {
            throw new IllegalStateException("Pending position count is not zero: " + pendingPositionCount);
        }
    }

    private void performIncrementalUpdate() {
        if (!pendingRemovedCursors.isEmpty()) {
            for (Node node : pendingRemovedCursors.values()) {
                removeNodeFromList(node);
                node.pendingRemove = false;
            }
            pendingRemovedCursors.clear();
        }
        if (pendingPositionCount == 0) {
            return;
        }
        for (Node node : cursors.values()) {
            if (node.pendingPosition != null) {
                if (node.position == null) {
                    node.position = node.pendingPosition;
                    insertNodeIntoList(node);
                } else {
                    moveNodeToNewPosition(node, node.position, node.pendingPosition);
                }
                node.pendingPosition = null;
                pendingPositionCount--;
            }
        }
    }

    // Rebuild the entire list from scratch for better performance with batch updates
    // This is more efficient than individual moves when many cursors need updating
    private void rebuildEntireList() {
        // Clear the existing list
        head = null;
        tail = null;
        trackedNodeCount = 0;

        // Collect all nodes that should be in the list and update their positions
        List<Node> activeNodes = new ArrayList<>();
        for (Node node : cursors.values()) {
            if (node.pendingPosition != null) {
                node.position = node.pendingPosition;
                node.pendingPosition = null;
                pendingPositionCount--;
            }
            if (node.position != null && !node.pendingRemove) {
                activeNodes.add(node);
                trackedNodeCount++;
            }
            node.pendingRemove = false;
        }

        pendingRemovedCursors.clear();

        // Sort nodes by position (maintaining insertion order for equal positions)
        activeNodes.sort((n1, n2) -> {
            int cmp = n1.position.compareTo(n2.position);
            if (cmp != 0) {
                return cmp;
            }
            // For equal positions, maintain the original order if possible
            return 0;
        });

        // Rebuild the linked list and counters
        Node prev = null;
        MutableInt currentCounter = null;
        Position lastPosition = null;
        int runningCount = 0;

        for (Node node : activeNodes) {
            // Link the node
            node.prev = prev;
            node.next = null;
            if (prev != null) {
                prev.next = node;
            } else {
                head = node;
            }

            // Update counters
            if (lastPosition == null || !node.position.equals(lastPosition)) {
                runningCount++;
                currentCounter = new MutableInt(runningCount);
                lastPosition = node.position;
            } else {
                currentCounter.increment();
                runningCount++;
            }
            node.numberOfCursorsAtSamePositionOrBefore = currentCounter;

            prev = node;
        }
        tail = prev;
    }

    @Override
    public Pair<Position, Position> cursorUpdated(ManagedCursor cursor, Position newPosition) {
        throw new UnsupportedOperationException("cursorUpdated method is not supported by this implementation");
    }

    /**
     * Move {@code node} from {@code oldPosition} to {@code newPosition} while
     * keeping the list ordered and maintaining the shared counters.
     *
     * The implementation updates counters only for the position-groups
     * crossed by the move, hence it is still O(distance) and faster than a
     * full remove + insert that would touch every node twice.
     */
    private void moveNodeToNewPosition(Node node, Position oldPosition, Position newPosition) {
        if (oldPosition.compareTo(newPosition) == 0) {
            return; // No change in position, nothing to do
        }
        boolean movingForward = oldPosition.compareTo(newPosition) < 0;

        // Check if the node is already in the correct position
        if (isAlreadyInCorrectPosition(node, newPosition)) {
            // Node is already in the correct position, no need to move the node

            // update the position
            node.position = newPosition;
            if (node == tail) {
                // Node is at the tail, no need to update counters
                return;
            }

            // update the counters
            if (movingForward) {
                // first decrement the counter for the old position
                node.numberOfCursorsAtSamePositionOrBefore.decrement();
            }
            if (node.prev != null && node.prev.position.compareTo(newPosition) == 0) {
                node.numberOfCursorsAtSamePositionOrBefore = node.prev.numberOfCursorsAtSamePositionOrBefore;
                // We joined the previous position group, so we need to increment the counter
                node.numberOfCursorsAtSamePositionOrBefore.increment();
            } else if (node.next != null && node.next.position.compareTo(newPosition) == 0) {
                node.numberOfCursorsAtSamePositionOrBefore = node.next.numberOfCursorsAtSamePositionOrBefore;
            } else {
                // If neither prev nor next has the same position, we need to recalculate the counter
                int base = node.prev != null ? node.prev.numberOfCursorsAtSamePositionOrBefore.intValue() : 0;
                node.numberOfCursorsAtSamePositionOrBefore = new MutableInt(base + 1);
            }
            return;
        }

        // ------------------------------------------------------------------
        // 1. Unlink the node from the list (but keep its prev/next for later
        //    traversal where needed).
        // ------------------------------------------------------------------
        if (node.prev != null) {
            node.prev.next = node.next;
        } else {
            head = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        } else {
            tail = node.prev;
        }

        Position lastProcessedPosition = null;

        if (movingForward) {
            // decrement the counter for the old position group
            node.numberOfCursorsAtSamePositionOrBefore.decrement();
            lastProcessedPosition = oldPosition;
            // ----------------------------------------------------------------
            // 2.a Decrement counters for every position-group strictly between
            //     (oldPosition, newPosition)
            // ----------------------------------------------------------------
            Node current = node.next;
            while (current != null && current.position.compareTo(newPosition) < 0) {
                if (current.position.compareTo(lastProcessedPosition) != 0) {
                    current.numberOfCursorsAtSamePositionOrBefore.decrement();
                    lastProcessedPosition = current.position;
                }
                current = current.next;
            }

            // current is the first node with position >= newPosition (or null)
            insertNodeAtNewPositionForward(node, current, newPosition);
        } else {
            // no need to change the counter for the old position group
            lastProcessedPosition = oldPosition;
            // moving backward
            // ----------------------------------------------------------------
            // 2.b Increment counters for every position-group >= oldPosition
            //     and > newPosition (we already decremented the oldPosition
            //     group right above).
            // ----------------------------------------------------------------
            Node current = node.prev;
            while (current != null && current.position.compareTo(newPosition) > 0) {
                if (current.position.compareTo(lastProcessedPosition) != 0) {
                    current.numberOfCursorsAtSamePositionOrBefore.increment();
                    lastProcessedPosition = current.position;
                }
                current = current.prev;
            }

            // current is the **last** node with position <= newPosition (or null)
            insertNodeAtNewPositionBackward(node, current, newPosition);
        }

        // finally update the node's stored position
        node.position = newPosition;
    }

    /**
     * Helper invoked when the cursor is moved forward
     * (newPosition > oldPosition).
     *
     * @param node    the node being moved (already detached)
     * @param current the first node whose position is >= newPosition, or null
     */
    private void insertNodeAtNewPositionForward(Node node, Node current, Position newPosition) {
        if (current == null) {
            // Insert at tail
            node.prev = tail;
            node.next = null;
            if (tail != null) {
                tail.next = node;
            } else {
                head = node;   // list was empty
            }
            tail = node;
            node.numberOfCursorsAtSamePositionOrBefore =
                    new MutableInt(node.prev.numberOfCursorsAtSamePositionOrBefore.intValue() + 1);
            return;
        }

        if (current.position.compareTo(newPosition) == 0) {
            // Join an existing position-group (keep insertion order before
            // the first element having that position)
            node.prev = current.prev;
            node.next = current;
            if (current.prev != null) {
                current.prev.next = node;
            } else {
                head = node;
            }
            current.prev = node;
            node.numberOfCursorsAtSamePositionOrBefore = current.numberOfCursorsAtSamePositionOrBefore;
            return;
        }

        // Create a new position-group *before* 'current'
        node.prev = current.prev;
        node.next = current;
        if (current.prev != null) {
            current.prev.next = node;
        } else {
            head = node;
        }
        current.prev = node;
        node.numberOfCursorsAtSamePositionOrBefore =
                new MutableInt(node.prev != null ? node.prev.numberOfCursorsAtSamePositionOrBefore.intValue() + 1 : 1);
    }

    /**
     * Helper invoked when the cursor is moved backward
     * (newPosition < oldPosition).
     *
     * @param node    the node being moved (already detached)
     * @param current the last node whose position is <= newPosition, or null
     */
    private void insertNodeAtNewPositionBackward(Node node, Node current, Position newPosition) {
        if (current == null) {
            // Insert at head
            node.prev = null;
            node.next = head;
            if (head != null) {
                head.prev = node;
            }
            head = node;
            if (tail == null) {
                tail = node;
            }
            node.numberOfCursorsAtSamePositionOrBefore = new MutableInt(1);
            return;
        }

        if (current.position.compareTo(newPosition) == 0) {
            // Append into an existing position-group (keep insertion order)
            node.prev = current;
            node.next = current.next;
            if (current.next != null) {
                current.next.prev = node;
            } else {
                tail = node;
            }
            current.next = node;
            node.numberOfCursorsAtSamePositionOrBefore = current.numberOfCursorsAtSamePositionOrBefore;
            node.numberOfCursorsAtSamePositionOrBefore.increment();
            return;
        }

        // Insert *after* 'current', creating a brand-new group
        node.prev = current;
        node.next = current.next;
        if (current.next != null) {
            current.next.prev = node;
        } else {
            tail = node;
        }
        current.next = node;

        node.numberOfCursorsAtSamePositionOrBefore =
                new MutableInt(current.numberOfCursorsAtSamePositionOrBefore.intValue() + 1);
    }

    /**
     * Check if a node is already in the correct position.
     */
    private boolean isAlreadyInCorrectPosition(Node node, Position newPosition) {
        if (node.prev != null && node.prev.position.compareTo(newPosition) > 0) {
            // Previous node has a greater position, so this node is not in the correct place
            return false;
        }
        if (node.next != null && node.next.position.compareTo(newPosition) < 0) {
            // Next node has a smaller position, so this node is not in the correct place
            return false;
        }
        return true;
    }

    private Position internalSlowestReaderPosition() {
        processPendingPositions();
        return head != null ? head.position : null;
    }

    @Override
    public Position getSlowestCursorPosition() {
        long stamp = rwLock.readLock();
        try {
            return internalSlowestReaderPosition();
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    @Override
    public boolean isEmpty() {
        long stamp = rwLock.tryOptimisticRead();
        boolean isEmpty = cursors.isEmpty();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                isEmpty = cursors.isEmpty();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        return isEmpty;
    }

    @Override
    public String toString() {
        long stamp = rwLock.readLock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append('[');

            boolean first = true;
            for (Node node : cursors.values()) {
                if (!first) {
                    sb.append(", ");
                }

                first = false;
                sb.append(node.cursor);
            }

            sb.append(']');
            return sb.toString();
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    @Override
    public Iterator<ManagedCursor> iterator() {
        final Iterator<Map.Entry<String, Node>> it = cursors.entrySet().iterator();
        return new Iterator<ManagedCursor>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public ManagedCursor next() {
                return it.next().getValue().cursor;
            }

            @Override
            public void remove() {
                throw new IllegalArgumentException("Cannot remove ManagedCursor from container");
            }
        };
    }

    @Override
    public int getNumberOfCursorsAtSamePositionOrBefore(ManagedCursor cursor) {
        long stamp = rwLock.writeLock();
        try {
            processPendingPositions();
            Node node = cursors.get(cursor.getName());
            if (node == null || node.position == null) {
                return 0;
            }
            return node.numberOfCursorsAtSamePositionOrBefore.intValue();
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public int size() {
        long stamp = rwLock.tryOptimisticRead();
        int size = cursors.size();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                size = cursors.size();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return size;
    }

    /**
     * Check the ordering and number of cursors count for each cursor.
     * This method is used for testing purposes to ensure that the internal state is consistent.
     */
    @VisibleForTesting
    void checkOrderingAndNumberOfCursorsState() {
        long stamp = rwLock.readLock();
        try {
            processPendingPositions();
            Node current = head;
            int currentCount = 0;
            Position lastPosition = null;
            List<String> lastPositionCursorNames = new ArrayList<>();
            while (current != null) {
                if (current.prev != null && current.position.compareTo(current.prev.position) < 0) {
                    throw new IllegalStateException("Cursors are not ordered: " + current.cursor.getName()
                            + " with position " + current.position + " is after cursor " + current.prev.cursor.getName()
                            + " with position " + current.prev.position);
                }
                currentCount++;
                if (lastPosition == null) {
                    lastPosition = current.position;
                    lastPositionCursorNames.add(current.cursor.getName());
                } else if (current.position.compareTo(lastPosition) > 0 || current.next == null) {
                    int expectedCount;
                    if (current.position.compareTo(lastPosition) <= 0) {
                        lastPositionCursorNames.add(current.cursor.getName());
                        lastPosition = current.position;
                        expectedCount = currentCount;
                    } else {
                        expectedCount = currentCount - 1;
                    }
                    if (current.prev.numberOfCursorsAtSamePositionOrBefore.intValue() != expectedCount) {
                        throw new IllegalStateException(
                                "Number of cursors at same position is not correct for position "
                                        + current.prev.position + ": expected " + expectedCount
                                        + ", but got " + current.prev.numberOfCursorsAtSamePositionOrBefore
                                        + " for cursors "
                                        + lastPositionCursorNames + " current.next: " + current.next);
                    }
                    if (current.next != null) {
                        lastPositionCursorNames.clear();
                        lastPositionCursorNames.add(current.cursor.getName());
                        lastPosition = current.position;
                    }
                } else {
                    lastPositionCursorNames.add(current.cursor.getName());
                }
                current = current.next;

            }
        } finally {
            rwLock.unlockRead(stamp);
        }
    }
}
