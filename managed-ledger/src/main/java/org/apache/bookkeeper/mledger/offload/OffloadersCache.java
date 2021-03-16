package org.apache.bookkeeper.mledger.offload;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of an Offloaders. The main purpose of this class is to
 * ensure that an Offloaders directory is only loaded once.
 */
@Slf4j
public class OffloadersCache implements AutoCloseable {

    private Map<String, Offloaders> loadedOffloaders = new ConcurrentHashMap<>();

    /**
     * Method to load an Offloaders directory or to get an already loaded Offloaders directory.
     *
     * @param offloadersPath - the directory to search the offloaders nar files
     * @param narExtractionDirectory - the directory to use for extraction
     * @return the loaded offloaders class
     * @throws IOException when fail to retrieve the pulsar offloader class
     */
    public Offloaders getOrLoadOffloaders(String offloadersPath, String narExtractionDirectory) {
        return loadedOffloaders.computeIfAbsent(offloadersPath,
                (directory) -> {
                    try {
                        return OffloaderUtils.searchForOffloaders(directory, narExtractionDirectory);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void close() {
        loadedOffloaders.values().forEach(offloaders -> {
            try {
                offloaders.close();
            } catch (Exception e) {
                log.error("Error while closing offloader.", e);
                // Even if the offloader fails to close, the graceful shutdown process continues
            }
        });
        // Don't want to hold on to references to closed offloaders
        loadedOffloaders.clear();
    }
}
