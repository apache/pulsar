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
package org.apache.pulsar.client.api.transaction;

import java.io.Serializable;
import java.util.Objects;
import lombok.Data;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * An identifier for representing a transaction.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@Data
public class TxnID implements Serializable {

    private static final long serialVersionUID = 0L;

    /*
     * The most significant 64 bits of this TxnID.
     *
     * @serial
     */
    private final long mostSigBits;

    /*
     * The least significant 64 bits of this TxnID.
     *
     * @serial
     */
    private final long leastSigBits;

    @Override
    public String toString() {
        return "(" + mostSigBits + "," + leastSigBits + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(mostSigBits, leastSigBits);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TxnID) {
            TxnID other = (TxnID) obj;
            return Objects.equals(mostSigBits, other.mostSigBits)
                    && Objects.equals(leastSigBits, other.leastSigBits);
        }

        return false;
    }
}
