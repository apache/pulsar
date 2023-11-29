# Testing Certificate authority

Generated based on instructions from https://jamielinux.com/docs/openssl-certificate-authority/introduction.html,
though the intermediate CA has been omitted for simplicity.

The following commands must be run in the same directory as this README due to the configuration for the openssl.cnf file.

The password for the CA private key is ```PulsarTesting```.

## Generating server keys

In this example, we're generating a key for the broker and the proxy. If there is a need to create them again, a new
CN will need to be used because we have the index.txt database in this directory. It's also possible that we could
remove this file and start over. At the time of adding this change, I didn't see a need to change the paradigm.

The common name when generating the CSR used to be the domain name of the broker. However, now we rely on the Subject
Alternative Name, or the SAN, to be the domain name. This is because the CN is deprecated in the certificate spec. The
[openssl.cnf](openssl.cnf) file has been updated to reflect this change. The proxy and the broker have the following
SAN: ```DNS:localhost, IP:127.0.0.1```.

```bash
openssl genrsa -out server-keys/broker.key.pem 2048
openssl req -config openssl.cnf -subj "/CN=broker-localhost-SAN" -key server-keys/broker.key.pem -new -sha256 -out server-keys/broker.csr.pem
openssl ca -config openssl.cnf -extensions broker_cert -days 100000 -md sha256 -in server-keys/broker.csr.pem \
    -out server-keys/broker.cert.pem -batch -key PulsarTesting
openssl pkcs8 -topk8 -inform PEM -outform PEM -in server-keys/broker.key.pem -out server-keys/broker.key-pk8.pem -nocrypt

openssl genrsa -out server-keys/proxy.key.pem 2048
openssl req -config openssl.cnf -subj "/CN=proxy-localhost-SAN" -key server-keys/proxy.key.pem -new -sha256 -out server-keys/proxy.csr.pem
openssl ca -config openssl.cnf -extensions proxy_cert -days 100000 -md sha256 -in server-keys/proxy.csr.pem \
    -out server-keys/proxy.cert.pem -batch -key PulsarTesting
openssl pkcs8 -topk8 -inform PEM -outform PEM -in server-keys/proxy.key.pem -out server-keys/proxy.key-pk8.pem -nocrypt
```

You need to configure the server with broker.key-pk8.pem and broker.cert.pem.

## Generating client keys

In the example, we're generating a key for the admin role. The only difference from server is the -extensions during CA step.
The common name when generating the CSR should be ```admin```.

```bash
openssl genrsa -out client-keys/admin.key.pem 2048
openssl req -config openssl.cnf -key client-keys/admin.key.pem -new -sha256 -out client-keys/admin.csr.pem
openssl ca -config openssl.cnf -extensions usr_cert \
    -days 1000 -notext -md sha256 -in client-keys/admin.csr.pem -out client-keys/admin.cert.pem
openssl pkcs8 -topk8 -inform PEM -outform PEM -in client-keys/admin.key.pem -out client-keys/admin.key-pk8.pem -nocrypt
```

The client is then configured with admin.key-pk8.pem and admin.cert.pem.

## JKS

The following command is used to generate the JKS certificate:
```shell
./generate_keystore.sh
```

These certificate type is JKS, password is 111111.
