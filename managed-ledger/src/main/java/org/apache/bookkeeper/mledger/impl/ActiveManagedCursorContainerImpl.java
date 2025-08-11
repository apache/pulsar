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
 * Contains cursors for a ManagedLedger.
 * <p>
 * The goal is to always know the slowest consumer and hence decide which is the oldest ledger we need to keep.
 * <p>
 * In addition, it allows to track the cursors that are before a given position, so that we can use this
 * information to calculate the expected read count for a read that is about to be performed. When
 * cacheEvictionByExpectedReadCount=true, the expected read count
 * is used for cached entries to determine how many possible times the entry is going to be read before eviction.
 * When the cache fills up, the eviction of entries with positive remaining expected read count will be postponed
 * until all other entries are evicted.
 */
@Slf4j
public class ActiveManagedCursorContainerImpl implements ActiveManagedCursorContainer {

    private static class Node {
        final ManagedCursor cursor;
        Position position;
        MutableInt numberOfCursorsAtSamePositionOrBefore;
        Node prev;
        Node next;

        Node(ManagedCursor cursor, Position position) {
            this.cursor = cursor;
            this.position = position;
        }
    }

    public ActiveManagedCursorContainerImpl() {}

    // Head of the double-linked list (sorted by position)
    private Node head = null;
    private Node tail = null;

    // Maps a cursor to the node
    private final ConcurrentMap<String, Node> cursors = new ConcurrentHashMap<>();

    private final StampedLock rwLock = new StampedLock();

    /**
     * Add a cursor to the container. The cursor will be optionally tracked for the slowest reader when
     * a position is passed as the second argument. It is expected that the position is updated with
     * {@link #cursorUpdated(ManagedCursor, Position)} method when the position changes.
     *
     * @param cursor cursor to add
     * @param position position of the cursor to use for ordering, pass null if the cursor's position shouldn't be
     *                 tracked for the slowest reader.
     */
    @Override
    public void add(ManagedCursor cursor, Position position) {
        long stamp = rwLock.writeLock();
        try {
            Node node = new Node(cursor, position);
            cursors.put(cursor.getName(), node);

            if (position != null) {
                insertNodeIntoList(node);
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
                    removeNodeFromList(node);
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

    /**
     * Signal that a cursor position has been updated and that the container must re-order the cursor heap
     * tracking the slowest reader.
     * Only those cursors are tracked and can be updated which were added to the container with the
     * {@link #add(ManagedCursor, Position)} method that specified the initial position in the position
     * parameter.
     *
     * @param cursor the cursor to update the position for
     * @param newPosition the updated position for the cursor
     * @return a pair of positions, representing the previous slowest reader and the new slowest reader (after the
     *         update).
     */
    @Override
    public Pair<Position, Position> cursorUpdated(ManagedCursor cursor, Position newPosition) {
        requireNonNull(cursor);

        long stamp = rwLock.writeLock();
        try {
            Node node = cursors.get(cursor.getName());
            if (node == null) {
                return null;
            }

            Position previousSlowestConsumer = internalSlowestReaderPosition();
            Position oldPosition = node.position;

            // Handle different cases
            if (oldPosition == null && newPosition != null) {
                // Node was not in the list, insert it
                node.position = newPosition;
                insertNodeIntoList(node);
            } else if (oldPosition != null && newPosition == null) {
                // Node was in the list, remove it
                removeNodeFromList(node);
                node.position = null;
            } else if (oldPosition != null && newPosition != null) {
                // Position changed, need to move the node
                moveNodeToNewPosition(node, oldPosition, newPosition);
            }
            // If both are null, nothing to do

            Position newSlowestConsumer = internalSlowestReaderPosition();
            return Pair.of(previousSlowestConsumer, newSlowestConsumer);
        } finally {
            rwLock.unlockWrite(stamp);
        }
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
            // Nothing to do – position didn't change
            return;
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
        return head != null ? head.position : null;
    }

    /**
     * Get the slowest reader position for the cursors that are ordered.
     *
     * @return the slowest reader position
     */
    @Override
    public Position getSlowestCursorPosition() {
        long stamp = rwLock.readLock();
        try {
            return internalSlowestReaderPosition();
        } finally {
            rwLock.unlockRead(stamp);
        }
    }

    /**
     *  Check whether there are any cursors.
     * @return true is there are no cursors and false if there are
     */
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
        long stamp = rwLock.readLock();
        try {
            Node node = cursors.get(cursor.getName());
            if (node == null || node.position == null) {
                return 0;
            }
            return node.numberOfCursorsAtSamePositionOrBefore.intValue();
        } finally {
            rwLock.unlockRead(stamp);
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
        Node current  = head;
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
                    throw new IllegalStateException("Number of cursors at same position is not correct for position "
                            + current.prev.position + ": expected " + expectedCount
                            + ", but got " + current.prev.numberOfCursorsAtSamePositionOrBefore + " for cursors "
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
    }
}
