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
package org.apache.pulsar.broker.loadbalance;

import java.util.List;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.ComputerSystem;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.Sensors;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

public final class OshiUtil {
    private static final SystemInfo systemInfo = new SystemInfo();
    private static final HardwareAbstractionLayer hardware;
    private static final OperatingSystem os;

    private OshiUtil() {
    }

    public static OperatingSystem getOs() {
        return os;
    }

    public static OSProcess getCurrentProcess() {
        return os.getProcess(os.getProcessId());
    }

    public static HardwareAbstractionLayer getHardware() {
        return hardware;
    }

    public static ComputerSystem getSystem() {
        return hardware.getComputerSystem();
    }

    public static GlobalMemory getMemory() {
        return hardware.getMemory();
    }

    public static CentralProcessor getProcessor() {
        return hardware.getProcessor();
    }

    public static Sensors getSensors() {
        return hardware.getSensors();
    }

    public static List<HWDiskStore> getDiskStores() {
        return hardware.getDiskStores();
    }

    public static List<NetworkIF> getNetworkIFs() {
        return hardware.getNetworkIFs();
    }

    static {
        hardware = systemInfo.getHardware();
        os = systemInfo.getOperatingSystem();
    }
}
