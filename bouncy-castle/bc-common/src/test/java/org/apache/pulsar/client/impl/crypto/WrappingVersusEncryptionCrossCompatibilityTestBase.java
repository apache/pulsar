/*
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
package org.apache.pulsar.client.impl.crypto;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Optional;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.SecurityUtility;
import org.hamcrest.Matcher;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class WrappingVersusEncryptionCrossCompatibilityTestBase {

    protected static SecretKey loadAESKEy(String aesKeyName)
            throws NoSuchAlgorithmException, NoSuchProviderException, IOException, InvalidKeySpecException,
            DecoderException {
        String s = FileUtils.readFileToString(
                Paths.get("../src/test/resources/certificate/" + aesKeyName + ".key").toFile(),
                StandardCharsets.UTF_8);

        return SecretKeyFactory.getInstance("AES", SecurityUtility.getProvider().getName())
                .generateSecret(new SecretKeySpec(Hex.decodeHex(s), "AES"));
    }


    public abstract Object[][] badEncryptionInputs()
            throws NoSuchAlgorithmException, IOException, InvalidKeySpecException, NoSuchProviderException,
            DecoderException;

    @DataProvider
    public Object[][] inputs()
            throws IOException, NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException,
            DecoderException {

        return new Object[][]{
                // public, private, aeskey, expectedEncryptedKey
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-2048.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-2048.pem", ImmutableMap.of()),
                        loadAESKEy("aes128bit"),
                        //encrypted by fips
                        Hex.decodeHex(
                                "A1903BEB5590222A3175871C1957041AE17848D0AD8B3A8667CC0033E016FE6AAA9DC9C8D7981DE18B9912604BC6B87BEC38688B42BA41F3296A8C27B2932017B01D9CA1814FE5F7676FA2C1900E5EDA1BA4D72D309FDE69C0993ADF9C1F1860420C24720242F3BC174D24F812C7404DD4E7F07E17CE181567D2C5BB63567336BC43BC9061C6A3B0722DC13F6DD4189889785AAAC3CFE84FCC574DCDDF6C59D20188571117C8046F84B6682B8C817FE2624D9744AE8B28748A6955855A13B1E1F2D64F162AB1627F0F488B61FCE7F7E4CDB27941C324C1D3BC3963213DD82616A97ED1A6A3B7089159BE4BC4704EC56E3F09F5BA063B2771FE9803DAC861D33C")
                },
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-2048.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-2048.pem", ImmutableMap.of()),
                        loadAESKEy("aes256bit"),
                        //encrypted by fips
                        Hex.decodeHex(
                                "458101A8694C77ECADCA9D0C77A01F71A2F8D26C5BB2F90111531E4E9F9662847703927AB1E122F95CE8851C99E9EE87BAD596895111D2B5C67336BA7B590C2F17C09B3B2886E8809F4E3AEEA643131244559E2930A4F1ABA34ECD9F1AB9553DEA4714199FB1CB72F5C0439F088BD3A001B94781A8B8C96129DF92935199A1FA977CD3F60CBD73BC9E96A8DC1583B9BB09FC8CE73DE87C74B6B7F7EEBF13EDD87012CD8CDED677F595D820F2B5F248A550E2286A4EAAD03DA074149C047296BB1DA79F80967DBA20AE28E0D44E251D1104248EA48CF822DE2CE99484C43E11D688243C4E995E75112265ACF340245BF901252198E4BD3E26EE7304B161908ED4")
                },
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-1024.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-1024.pem", ImmutableMap.of()),
                        loadAESKEy("aes128bit"),
                        //encrypted by fips
                        Hex.decodeHex(
                                "AA96053587FCD7329F91B1E285981A7D3C07185283D248881636FE772C5FAE414C1B8D782E512CAF5326312E39F01546C7E5BCCBDF26D4AA88EBA176503546DBFAE3EADDA92AF74C159AFF6E936DA48CA6E643F2F973DC2B8B1B9DDDBD81DD4B03435E660573A856B599A624E584B363A23F88C3D864C9FFE7F988E52C78663C")
                },
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-1024.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-1024.pem", ImmutableMap.of()),
                        loadAESKEy("aes256bit"),
                        //encrypted by fips
                        Hex.decodeHex(
                                "B4C0AF5FAAA2ECF1E96978545729F89E2D70350CB6220940ACC1613C4E03EB0249303CD5546168CFADB0E52A3D9522BEECA57F353DF2F43EEBD1AD970F962C5D5E17D8DAAC7825A66B3A81A651F8930A96E6A3628125AF2864A545A0103878CD56A8EEDAB0592982F940F0892C2B4803EA4D60FE581F52B34A164534BD26892E")
                },
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-2048.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-2048.pem", ImmutableMap.of()),
                        loadAESKEy("aes128bit"),
                        //encrypted by non-fips
                        Hex.decodeHex(
                                "2B4C78D3ADC80834FDC51D9D03FB42D50BD05A1F042A8F2CF07AB0BC503AF3354124D83D9A25B5BA8ACA7723C3B2CCE9836C1A5F82C967B8B23255561CFB1E9D115A68BA7AB583BFE28339937F4886E7CD94D79D47B2D483E6EBAABEA30264A86F9A7C6E52E4ECC38967F8B58385CE2BDB3500625C5B113FED029D07401278A9D9D1E16E01B3260E68310FC6FC84FCE459FECAD48BC0ACA3B4DB3890EB859D4D8D327323ACE6B5A79ADE5BD3FE46AE1DFC946B81F74E61A5914B269429F5C1A4AFC00FF02DE2E95DD5432BC6301425C7EF54F8B04FD8E238570B5BFAFF48078FCBF591EBA86875272DC50E2335FACC635D32E42AAF6F8675F91F664CE936B217")
                },
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-2048.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-2048.pem", ImmutableMap.of()),
                        loadAESKEy("aes256bit"),
                        //encrypted by non-fips
                        Hex.decodeHex(
                                "6F164F075AFCE3316B150F4B48C688A865EFD5360F77778517441578952CC3952ACE7DF19A3F3796F9E2F31E1E4A92DE915BE0E8EED2B2EDF87F550F29BB902A3EC5000146F65A0ED97D195AB643B8ACEC3E8CF6A32F621CC5E9FB067F396C6F4222DE4F9A7CE21A5FC5F4BD6CDC1651D23C4FBC810F686E9BA15C9AA2C3EF733CC26B83E8A480E2CC4967821BE3D228C557A51421544A70B8D392E384851DAD78035A504646CEE1097952782D6EDB81FDD45238E4BD25F0AC3416EBD22307E2427DF90F2CE29B2F3237A57F2E71470D2C2C17ECC87B39AC429613AD45016570A921446D92DFEC0F5C58FD26D36DD9A3E693F4C2933326DDE9745C6636F9EE83")
                },
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-1024.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-1024.pem", ImmutableMap.of()),
                        loadAESKEy("aes128bit"),
                        //encrypted by non-fips
                        Hex.decodeHex(
                                "20661CFCF9ADF39FB5B3F59EB7FB7AAD16E28C838F96A34887534CB95CCA14259A95B1759AE16C3C8138F15992BF2DA5AFDC32EFFAA9FDC98C0C59569AC61A0B7BF94090D15AD17812503B2E03C81D948928EEC220E15362764833F5D5A09050472CCE808415AD3EFF17C19B6899A45BAE2F3649F2D108CB9EBBFC2B16CEE568")
                },
                {
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PUBLIC, "rsa-1024.pem", ImmutableMap.of()),
                        EncKeyReader.getKeyAsPEM(EncKeyReader.KeyType.PRIVATE, "rsa-1024.pem", ImmutableMap.of()),
                        loadAESKEy("aes256bit"),
                        //encrypted by non-fips
                        Hex.decodeHex(
                                "1AD23E7D9A8546A8F8D5308F8DA0643334F63DAD2AE15FDBE5434FADC697FDD4DBCFDD9D43CD0FFA978AE1C6C140361D9024CA6FCBF250CE12D9B4FF64B6FDB4DE408CA327872549EE33B23D48ACA3A48A0BC401DC386E99FDCEC1B955AD49FD4B9F7F8D86B6AD892382B472E181E46FC61AC51511BEC0C26419BAABAA8B7069")
                }
        };
    }

    @Test(dataProvider = "inputs")
    public void testDecryptingDataKey(byte[] rsaPublicKeyAsPEM,
                                      byte[] rsaPrivateKeyAsPEM,
                                      SecretKey aesKey,
                                      byte[] expectedEncryptedKey) {
        BcVersionSpecificCryptoUtility bcFipsSpecificUtility = BcVersionSpecificCryptoUtility.INSTANCE;

        PrivateKey key = bcFipsSpecificUtility.loadPrivateKey(rsaPrivateKeyAsPEM);
        Optional<SecretKey> resultingKey =
                bcFipsSpecificUtility
                        .deCryptDataKey("AES", "logCtx", "the_private_key", key, expectedEncryptedKey);

        assertEquals(aesKey.getEncoded(), resultingKey.get().getEncoded());
    }

    @Test(dataProvider = "inputs")
    public void testEncryptingDataKeyWithFipsMatchesWithNonFipsOutput(byte[] rsaPublicKeyAsPEM,
                                                                      byte[] rsaPrivateKeyAsPEM,
                                                                      SecretKey aesKey,
                                                                      byte[] expectedEncryptedKey)
            throws PulsarClientException.CryptoException {

        BcVersionSpecificCryptoUtility bcFipsSpecificUtility = BcVersionSpecificCryptoUtility.INSTANCE;

        PublicKey pubKey = bcFipsSpecificUtility.loadPublicKey(rsaPublicKeyAsPEM);
        PrivateKey privKey = bcFipsSpecificUtility.loadPrivateKey(rsaPrivateKeyAsPEM);
        byte[] actual = bcFipsSpecificUtility.encryptDataKey("", "", pubKey, aesKey);

//      // to capture outputs for further tests
//        System.out.printf("%d bit AES encryption with %d bit RSA pubkey result: %s %n",aesKey.getEncoded()
//         .length*8,pubKey.getEncoded().length*8,bytesToHex(actual));

        //encryption is non-deterministic, so we can only check that a full roundtrip matches with starting element
        Optional<SecretKey> roundtripDecodedData = bcFipsSpecificUtility.deCryptDataKey("AES", "", "", privKey, actual);
        assertEquals(aesKey.getEncoded(), roundtripDecodedData.get().getEncoded());
    }

    @Test(dataProvider = "badEncryptionInputs")
    public void testBadEncryptionInputs(byte[] rsaPublicKeyAsPEM,
                                        byte[] rsaPrivateKeyAsPEM,
                                        SecretKey aesKey,
                                        Matcher<? super Throwable> expectedException) {
        try {
            testEncryptingDataKeyWithFipsMatchesWithNonFipsOutput(rsaPublicKeyAsPEM, rsaPrivateKeyAsPEM, aesKey,
                    new byte[]{});
        } catch (Throwable t) {
            assertThat(t, expectedException);
        }
    }

    /**
     * How to generate new keys
     */
    public static void main(String[] args) throws NoSuchAlgorithmException, NoSuchProviderException, DecoderException {

        //this loads provider
        final String algorithm = "AES";

        for (int keySize : new int[]{128, 256}) {
            KeyGenerator keyGen = KeyGenerator.getInstance(algorithm);
            keyGen.init(keySize);
            SecretKey aesKey = keyGen.generateKey();
            String keyAsHexString = Hex.encodeHexString(aesKey.getEncoded());
            System.out.printf("%s%dBitKey = %s%n", algorithm,
                    keySize,
                    keyAsHexString);
            assertEquals(aesKey.getEncoded(), Hex.decodeHex(keyAsHexString));
        }

        /*
        generate RSA keys:
        openssl genrsa -out private-key.rsa-2048.pem 2048
        openssl rsa -in private-key.rsa-2048.pem -outform PEM -pubout -out public-key.rsa-2048.pem
        openssl genrsa -out private-key.rsa-256.pem 256
        openssl rsa -in private-key.rsa-256.pem -outform PEM -pubout -out public-key.rsa-256.pem
        openssl genrsa -out private-key.rsa-1024.pem 1024
        openssl rsa -in private-key.rsa-1024.pem -outform PEM -pubout -out public-key.rsa-1024.pem
        */
    }

}
