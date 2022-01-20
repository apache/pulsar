package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Progress;

public class ProgressManager {

    public ProgressInfo persistentProgressInfos;

    public ProgressInfo pushedProgressInfos;

    public void updatePersistentProgress(Progress progress, long sequenceId) {
        persistentProgressInfos.setProgress(progress);
        persistentProgressInfos.setSequenceId(sequenceId);
    }

    public void updatePushedProgress(Progress progress, long sequenceId) {
        pushedProgressInfos.setProgress(progress);
        pushedProgressInfos.setSequenceId(sequenceId);
    }

    public ProgressInfo getPublishedProgressInfos() {
        return pushedProgressInfos;
    }

    public ProgressInfo getPersistentProgressInfos() {
        return persistentProgressInfos;
    }

    public ProgressInfo loadProgress(byte[] progressBuf){
        // recover progress from progressbuf
        persistentProgressInfos = persistentProgressInfos = null;
        return persistentProgressInfos;
    }

    public byte[] saveProgress(){
        return null;
    }

}
