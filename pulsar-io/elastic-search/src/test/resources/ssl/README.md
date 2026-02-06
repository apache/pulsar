# SSL Cert Provenance

The files were generated with the following steps. They are not in a script because a script likely won't
work the next time these files need to be updated. These files were copied out of convenience.

One important assumption is that all certs and keystores share the `cacert.pem` as a root CA.

[cacert.pem](./cacert.pem) was copied from the tests/certificate-authority/certs/ca.cert.pem file.
```shell
cp ../../../../../../tests/certificate-authority/certs/ca.cert.pem cacert.pem
```

[cacert.crt](./cacert.crt) was generated using the following command:
```shell
openssl x509 -in cacert.pem -inform pem -out cacert.crt -outform der
```

The [truststore.jks](./truststore.jks) file was generated using the following command:
```shell
keytool -importcert -alias rootca -keystore truststore.jks -storepass changeit -file cacert.crt -noprompt
```

The [keystore.jks](./keystore.jks) file was generated using the following commands:
```shell
cat ../../../../../../tests/certificate-authority/client-keys/admin.cert.pem > client.pem
cat ../../../../../../tests/certificate-authority/client-keys/admin.key.pem >> client.pem
openssl pkcs12 -export -in client.pem -out client.p12
```

Manually enter `123456` password.

```shell
keytool -importkeystore -srckeystore client.p12 -srcstoretype pkcs12 -srcstorepass 123456 -destkeystore keystore.jks -deststorepass changeit -noprompt
rm client.pem client.p12
```

The [elasticsearch.crt](./elasticsearch.crt), [elasticsearch.key](./elasticsearch.key), [elasticsearch.pem](./elasticsearch.pem) files were all copied from broker certs.

```shell
cp ../../../../../../tests/certificate-authority/server-keys/broker.cert.pem elasticsearch.crt
cp ../../../../../../tests/certificate-authority/server-keys/broker.key.pem elasticsearch.key
cp ../../../../../../tests/certificate-authority/server-keys/broker.key-pk8.pem elasticsearch.pem
```