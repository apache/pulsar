#include "utils.h"

CryptoKeyReaderWrapper::CryptoKeyReaderWrapper() {}

CryptoKeyReaderWrapper::CryptoKeyReaderWrapper(const std::string& publicKeyPath,
                                             const std::string& privateKeyPath) {
    this->cryptoKeyReader = DefaultCryptoKeyReader::create(publicKeyPath, privateKeyPath);
}

void export_cryptoKeyReader() {
    using namespace boost::python;

    class_<CryptoKeyReaderWrapper>("CryptoKeyReader", init<const std::string&, const std::string&>());
}