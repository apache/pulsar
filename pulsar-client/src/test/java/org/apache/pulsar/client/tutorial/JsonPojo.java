package org.apache.pulsar.client.tutorial;

public class JsonPojo {
    public String content;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public JsonPojo() {
    }

    public JsonPojo(String content) {
        this.content = content;
    }
}
