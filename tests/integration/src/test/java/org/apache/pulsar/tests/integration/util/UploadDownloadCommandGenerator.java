package org.apache.pulsar.tests.integration.util;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class UploadDownloadCommandGenerator {
    public enum MODE {
        UPLOAD,
        DOWNLOAD,
    };
    private MODE mode;
    private String bkPath;
    private String localPath;
    private String brokerUrl;

    public static UploadDownloadCommandGenerator createUploader(String localPath, String bkPath) {
        return new UploadDownloadCommandGenerator(MODE.UPLOAD, localPath, bkPath);
    }

    public static UploadDownloadCommandGenerator createDownloader(String localPath, String bkPath) {
        return new UploadDownloadCommandGenerator(MODE.DOWNLOAD, localPath, bkPath);
    }

    public UploadDownloadCommandGenerator(MODE mode, String localPath, String bkPath) {
        this.mode = mode;
        this.localPath = localPath;
        this.bkPath = bkPath;
    }

    public void createBrokerUrl(String host, int port) {
        brokerUrl = "pulsar://" + host + ":" + port;
    }

    public String generateCommand() {
        StringBuilder commandBuilder = new StringBuilder("PULSAR_MEM=-Xmx1024m /pulsar/bin/pulsar-admin functions ");
        if (mode == MODE.UPLOAD) {
            commandBuilder.append(" upload ");
        } else {
            commandBuilder.append(" download ");
        }
        commandBuilder.append(" --path ");
        commandBuilder.append(bkPath);
        if (mode == MODE.UPLOAD) {
            commandBuilder.append(" --sourceFile ");
        } else {
            commandBuilder.append(" --destinationFile ");
        }
        commandBuilder.append(localPath);
        return commandBuilder.toString();
    }
}
