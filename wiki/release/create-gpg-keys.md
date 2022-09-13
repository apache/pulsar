<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Create GPG keys to sign release artifacts

This page provides instructions for Pulsar committers on how to do
the initial gpg setup.

This is a condensed version of instructions available at
http://apache.org/dev/openpgp.html.


Install GnuPG. For example on MacOS:

```shell
brew install gnupg
```

Set configuration to use `SHA512` keys by default.

```shell
mkdir ~/.gnupg
cat <<EOL >> ~/.gnupg/gpg.conf
personal-digest-preferences SHA512
cert-digest-algo SHA512
default-preference-list SHA512 SHA384 SHA256 SHA224 AES256 AES192 AES CAST5 ZLIB BZIP2 ZIP Uncompressed
EOL
chmod 700 ~/.gnupg/gpg.conf
```

Check the version.

```shell
gpg --version

gpg (GnuPG) 2.1.22
libgcrypt 1.8.0
Copyright (C) 2017 Free Software Foundation, Inc.
License GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Home: /Users/nkurihar/.gnupg
Supported algorithms:
Pubkey: RSA, ELG, DSA, ECDH, ECDSA, EDDSA
Cipher: IDEA, 3DES, CAST5, BLOWFISH, AES, AES192, AES256, TWOFISH,
        CAMELLIA128, CAMELLIA192, CAMELLIA256
Hash: SHA1, RIPEMD160, SHA256, SHA384, SHA512, SHA224
Compression: Uncompressed, ZIP, ZLIB, BZIP2
```

Generate new GPG key.
Note that new **RSA** keys generated should be at least **4096** bits.

```shell
# For 1.x or 2.0.x
gpg --gen-key

# For 2.1.x
gpg --full-gen-key

gpg (GnuPG) 2.1.22; Copyright (C) 2017 Free Software Foundation, Inc.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Please select what kind of key you want:
   (1) RSA and RSA (default)
   (2) DSA and Elgamal
   (3) DSA (sign only)
   (4) RSA (sign only)
Your selection? 1
RSA keys may be between 1024 and 4096 bits long.
What keysize do you want? (2048) 4096
Requested keysize is 4096 bits       
Please specify how long the key should be valid.
         0 = key does not expire
      <n>  = key expires in n days
      <n>w = key expires in n weeks
      <n>m = key expires in n months
      <n>y = key expires in n years
Key is valid for? (0) 0
Key does not expire at all
Is this correct? (y/N) y
                        
GnuPG needs to construct a user ID to identify your key.

Real name: test user
Email address: test@apache.org
Comment: CODE SIGNING KEY     
You selected this USER-ID:
    "test user (CODE SIGNING KEY) <test@apache.org>"

Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit? O
<Enter passphrase>
```

#### Appending the key to KEYS files

The GPG key needs to be appended to `KEYS` file that is stored in 2 SVN locations,
one for proper releases and one for the release candidates.

The credentials for SVN are the usual Apache account credentials.

```shell
# Checkout the SVN folder containing the KEYS file
svn co https://dist.apache.org/repos/dist/dev/pulsar pulsar-dist-dev-keys --depth empty
cd pulsar-dist-dev-keys
svn up KEYS

APACHEID=apacheid
# Export the key in ascii format and append it to the file
( gpg --list-sigs $APACHEID@apache.org
  gpg --export --armor $APACHEID@apache.org ) >> KEYS

# Commit to SVN
svn ci -m "Added gpg key for $APACHEID"
```

Repeat the same operation for the release KEYS file:

> :warning: You should ask a PMC member to complete this step.

```shell
# Checkout the SVN folder containing the KEYS file
svn co https://dist.apache.org/repos/dist/release/pulsar pulsar-dist-release-keys --depth empty
cd pulsar-dist-release-keys
svn up KEYS

APACHEID=apacheid
# Export the key in ascii format and append it to the file
( gpg --list-sigs $APACHEID@apache.org
  gpg --export --armor $APACHEID@apache.org ) >> KEYS

# Commit to SVN
svn ci -m "Added gpg key for $APACHEID"
```

#### Upload the key to a public key server

Use the key id to publish it to several public key servers:
```shell
gpg --send-key 8C75C738C33372AE198FD10CC238A8CAAC055FD2
gpg --send-key --keyserver=keys.openpgp.org 8C75C738C33372AE198FD10CC238A8CAAC055FD2
gpg --send-key --keyserver=keyserver.ubuntu.com 8C75C738C33372AE198FD10CC238A8CAAC055FD2
```