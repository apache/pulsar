package org.apache.pulsar.io.azuredataexplorer;

import java.sql.Timestamp;

public class ADXPulsarEvent {
    public String Key;
    public String Value;
    public String Properties;
    public String ProducerName;
    public long SequenceId;
    public Timestamp EventTime;
}
