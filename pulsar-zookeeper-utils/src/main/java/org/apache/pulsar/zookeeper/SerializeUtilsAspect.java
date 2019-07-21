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
package org.apache.pulsar.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.CreateTxnV0;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class SerializeUtilsAspect {
    @Around("execution(* org.apache.zookeeper.server.util.SerializeUtils.deserializeTxn(..))")
    public Record wrapper(ProceedingJoinPoint joinPoint) throws IOException {
        byte[] txnBytes = (byte[]) joinPoint.getArgs()[0];
        TxnHeader hdr = (TxnHeader) joinPoint.getArgs()[1];

        return deserializeTxn(txnBytes, hdr);
    }

    private static final int CREATE2 = 15;
    private static final int CREATE_CONTAINER = 19;
    private static final int DELETE_CONTAINER = 20;

    /**
     * Copied `deserializeTxn()` from ZK-3.4.13
     *
     * https://github.com/apache/zookeeper/blob/release-3.4.13/src/java/main/org/apache/zookeeper/server/util/SerializeUtils.java#L54
     *
     * With addition for handling `create2`, `createContainer` and `deleteContainer` transactions when downgrading from
     * 3.5.x to 3.4.x
     */
    public static Record deserializeTxn(byte txnBytes[], TxnHeader hdr)
            throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
        InputArchive ia = BinaryInputArchive.getArchive(bais);

        hdr.deserialize(ia, "hdr");
        bais.mark(bais.available());
        Record txn = null;
        switch (hdr.getType()) {
        case OpCode.createSession:
            // This isn't really an error txn; it just has the same
            // format. The error represents the timeout
            txn = new CreateSessionTxn();
            break;
        case OpCode.closeSession:
            return null;
        case OpCode.create:
        case CREATE2: // create2 is treated as a create op in 3.5.x
        case CREATE_CONTAINER: // createContainer can be treated as a regular create operation, since ZK 3.4 doens't
                               // have support for auto cleaning the empty directories
            txn = new CreateTxn();
            break;
        case OpCode.delete:
        case DELETE_CONTAINER: // deleteContainer is treaded as regular delete in 3.5.x
            txn = new DeleteTxn();
            break;
        case OpCode.setData:
            txn = new SetDataTxn();
            break;
        case OpCode.setACL:
            txn = new SetACLTxn();
            break;
        case OpCode.error:
            txn = new ErrorTxn();
            break;
        case OpCode.multi:
            txn = new MultiTxn();
            break;
        default:
            throw new IOException("Unsupported Txn with type=%d" + hdr.getType());
        }
        if (txn != null) {
            try {
                txn.deserialize(ia, "txn");
            } catch(EOFException e) {
                // perhaps this is a V0 Create
                if (hdr.getType() == OpCode.create) {
                    CreateTxn create = (CreateTxn)txn;
                    bais.reset();
                    CreateTxnV0 createv0 = new CreateTxnV0();
                    createv0.deserialize(ia, "txn");
                    // cool now make it V1. a -1 parentCVersion will
                    // trigger fixup processing in processTxn
                    create.setPath(createv0.getPath());
                    create.setData(createv0.getData());
                    create.setAcl(createv0.getAcl());
                    create.setEphemeral(createv0.getEphemeral());
                    create.setParentCVersion(-1);
                } else {
                    throw e;
                }
            }
        }
        return txn;
    }

}
