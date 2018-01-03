package org.apache.pulsar.functions.runtime.worker;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.net.URI;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
public class PackageLocationMetaData implements Serializable{
    private String packageName;
    private String packageNamespace;
    private String dlogUri;

    public URI getPackageNamespaceURI() {
        URI baseUri = URI.create(this.dlogUri);
        String zookeeperHost = baseUri.getHost();
        int zookeeperPort = baseUri.getPort();
        return URI.create(
                String.format("distributedlog://%s:%d/%s",
                        zookeeperHost, zookeeperPort, this.packageNamespace));
    }

    public String getPackageURI() {
        return String.format("%s/%s", this.getPackageNamespaceURI().toString(), this.packageName);
    }

}
