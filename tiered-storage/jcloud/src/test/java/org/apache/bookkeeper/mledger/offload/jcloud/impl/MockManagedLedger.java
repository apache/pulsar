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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import com.google.common.base.Predicate;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

@Slf4j
public class MockManagedLedger implements ManagedLedger {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public Position addEntry(byte[] data) {
        return null;
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages) {
        return null;
    }

    @Override
    public void asyncAddEntry(byte[] data, AsyncCallbacks.AddEntryCallback callback, Object ctx) {

    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) {
        return null;
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length) throws InterruptedException,
            ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {

    }

    @Override
    public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length,
                              AsyncCallbacks.AddEntryCallback callback, Object ctx) {

    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AsyncCallbacks.AddEntryCallback callback, Object ctx) {

    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {

    }

    @Override
    public ManagedCursor openCursor(String name) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition) throws
            InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                    Map<String, Long> properties) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName) throws
            ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName,
                                             CommandSubscribe.InitialPosition initialPosition) throws
            ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncDeleteCursor(String name, AsyncCallbacks.DeleteCursorCallback callback, Object ctx) {

    }

    @Override
    public void deleteCursor(String name) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {

    }

    @Override
    public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                AsyncCallbacks.OpenCursorCallback callback, Object ctx) {

    }

    @Override
    public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                Map<String, Long> properties, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {

    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return null;
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return null;
    }

    @Override
    public long getNumberOfEntries() {
        return 0;
    }

    @Override
    public long getNumberOfActiveEntries() {
        return 0;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public long getEstimatedBacklogSize() {
        return 0;
    }

    @Override
    public long getOffloadedSize() {
        return 0;
    }

    @Override
    public long getLastOffloadedLedgerId() {
        return 0;
    }

    @Override
    public long getLastOffloadedSuccessTimestamp() {
        return 0;
    }

    @Override
    public long getLastOffloadedFailureTimestamp() {
        return 0;
    }

    @Override
    public void asyncTerminate(AsyncCallbacks.TerminateCallback callback, Object ctx) {

    }

    @Override
    public Position terminate() throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {

    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return null;
    }

    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDelete(AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {

    }

    @Override
    public Position offloadPrefix(Position pos) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncOffloadPrefix(Position pos, AsyncCallbacks.OffloadCallback callback, Object ctx) {

    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return null;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return null;
    }

    @Override
    public void setConfig(ManagedLedgerConfig config) {

    }

    @Override
    public Position getLastConfirmedEntry() {
        return null;
    }

    @Override
    public void readyToCreateNewLedger() {

    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncSetProperty(String key, String value, AsyncCallbacks.UpdatePropertiesCallback callback,
                                 Object ctx) {

    }

    @Override
    public void deleteProperty(String key) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDeleteProperty(String key, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {

    }

    @Override
    public void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncSetProperties(Map<String, String> properties,
                                   AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {

    }

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {

    }

    @Override
    public void rollCurrentLedgerIfFull() {

    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        return null;
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        return null;
    }

    @Override
    public CompletableFuture<LedgerInfo> getLedgerInfo(long ledgerId) {
        final LedgerInfo build = LedgerInfo.newBuilder().setLedgerId(ledgerId).setSize(100).setEntries(20).build();
        return CompletableFuture.completedFuture(build);
    }

    @Override
    public CompletableFuture<Void> asyncTruncate() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) {
        return null;
    }
}
