/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <fstream>
#include <sstream>
#include <pulsar/EncryptionKeyInfo.h>
#include <pulsar/CryptoKeyReader.h>
#include <pulsar/Result.h>

using namespace pulsar;

CryptoKeyReader::CryptoKeyReader() {}
CryptoKeyReader::~CryptoKeyReader() {}

Result CryptoKeyReader::getPublicKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                                     EncryptionKeyInfo& encKeyInfo) const {
    return ResultInvalidConfiguration;
}

Result CryptoKeyReader::getPrivateKey(const std::string& keyName,
                                      std::map<std::string, std::string>& metadata,
                                      EncryptionKeyInfo& encKeyInfo) const {
    return ResultInvalidConfiguration;
}

DefaultCryptoKeyReader::DefaultCryptoKeyReader(const std::string& publicKeyPath,
                                               const std::string& privateKeyPath) {
    publicKeyPath_ = publicKeyPath;
    privateKeyPath_ = privateKeyPath;
}

DefaultCryptoKeyReader::~DefaultCryptoKeyReader() {}

void DefaultCryptoKeyReader::readFile(std::string fileName, std::string& fileContents) const {
    std::ifstream ifs(fileName);
    std::stringstream fileStream;
    fileStream << ifs.rdbuf();

    fileContents = fileStream.str();
}

Result DefaultCryptoKeyReader::getPublicKey(const std::string& keyName,
                                            std::map<std::string, std::string>& metadata,
                                            EncryptionKeyInfo& encKeyInfo) const {
    std::string keyContents;
    readFile(publicKeyPath_, keyContents);

    encKeyInfo.setKey(keyContents);
    return ResultOk;
}

Result DefaultCryptoKeyReader::getPrivateKey(const std::string& keyName,
                                             std::map<std::string, std::string>& metadata,
                                             EncryptionKeyInfo& encKeyInfo) const {
    std::string keyContents;
    readFile(privateKeyPath_, keyContents);

    encKeyInfo.setKey(keyContents);
    return ResultOk;
}

CryptoKeyReaderPtr DefaultCryptoKeyReader::create(const std::string& publicKeyPath,
                                                  const std::string& privateKeyPath) {
    return CryptoKeyReaderPtr(new DefaultCryptoKeyReader(publicKeyPath, privateKeyPath));
}