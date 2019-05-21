package org.apache.bookkeeper.mledger.offload.filesystem;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.offload.filesystem.impl.FileSystemManagedLedgerOffloader;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class FileSystemLedgerOffloaderFactory implements LedgerOffloaderFactory<FileSystemManagedLedgerOffloader> {
    @Override
    public boolean isDriverSupported(String driverName) {
        return FileSystemManagedLedgerOffloader.driverSupported(driverName);
    }

    @Override
    public FileSystemManagedLedgerOffloader create(Properties properties, Map<String, String> userMetadata, OrderedScheduler scheduler) throws IOException {
        TieredStorageConfigurationData data = TieredStorageConfigurationData.create(properties);
        return FileSystemManagedLedgerOffloader.create(data, userMetadata, scheduler);
    }

}
