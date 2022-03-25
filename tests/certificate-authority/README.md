# Testing Certificate authority

Generated based on instructions from https://jamielinux.com/docs/openssl-certificate-authority/introduction.html,
though the intermediate CA has been omitted for simplicity.

The environment variable, CA_HOME, must be set to point to the directory
containing this file before running any openssl commands.

The password for the CA private key is ```PulsarTesting```.

## Generating server keys

In this example, we're generating a key for the broker.

The common name when generating the CSR should be the domain name of the broker.

```bash
openssl genrsa -out server-keys/broker.key.pem 2048
openssl req -config openssl.cnf -key server-keys/broker.key.pem -new -sha256 -out server-keys/broker.csr.pem
openssl ca -config openssl.cnf -extensions server_cert \
    -days 100000 -notext -md sha256 -in server-keys/broker.csr.pem -out server-keys/broker.cert.pem
openssl pkcs8 -topk8 -inform PEM -outform PEM -in server-keys/broker.key.pem -out server-keys/broker.key-pk8.pem -nocrypt
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


