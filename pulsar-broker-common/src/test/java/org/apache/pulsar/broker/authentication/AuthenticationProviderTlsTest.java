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
package org.apache.pulsar.broker.authentication;

import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.math.BigInteger;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.assertEquals;


/**
 * Unit test for {@link AuthenticationProviderTls}.
 */
public class AuthenticationProviderTlsTest {

    private static final String ROOT_CN = "Tony Stark";
    private static final String ROOT_OU = "CEO";
    private static final String ROOT_O = "Stark Industries";

    private static final String INTERMEDIATE_CN = "*.BruceWayne.com";
    private static final String INTERMEDIATE_OU = "Chairman";
    private static final String INTERMEDIATE_O = "Wayne Enterprises, Inc.";

    private static final String CLIENT_CN= "Phillip J Fry";
    private static final String CLIENT_OU= "Delivery Dept.";
    private static final String CLIENT_O= "Planet Express, Inc.";


    private X509Certificate[] chain;

    @BeforeClass
    public void setUpCertificateChain() throws CertificateException, NoSuchAlgorithmException,
            OperatorCreationException, CertIOException {
        chain = generateCertificateChain(new String[][] {
                new String[] {ROOT_CN, ROOT_OU, ROOT_O},
                new String[] {INTERMEDIATE_CN, INTERMEDIATE_OU, INTERMEDIATE_O},
                new String[] {CLIENT_CN, CLIENT_OU, CLIENT_O}},
                30000, 30000);

    }

    @BeforeMethod
    public void resetStatistics() {
        AuthenticationProviderTls.resetMetrics();
    }

    @Test
    public void testPrintCertificateChain() throws CertificateException, NoSuchAlgorithmException,
            CertIOException, OperatorCreationException {
        assertEquals("O=Stark Industries,OU=CEO,CN=Tony Stark -> O=Wayne Enterprises\\, Inc.,OU=Chairman,CN=*.BruceWayne.com -> O=Planet Express\\, Inc.,OU=Delivery Dept.,CN=Phillip J Fry",
                AuthenticationProviderTls.concatenateFullCertChain(chain));
    }

    @Test
    public void testCheckIfWildcardCertificate() {
        AuthenticationProviderTls.checkIfWildcardCertificate(chain[1], INTERMEDIATE_CN);
        assertEquals(1, AuthenticationProviderTls.clientCertWildcardMetrics.get(), 0);
    }

    @Test
    public void testCheckIfSelfSignedCertificate() {
        AuthenticationProviderTls.checkIfSelfSignedCertificate(chain[chain.length-1], ROOT_CN);
        assertEquals(1, AuthenticationProviderTls.clientCertSelfSignedMetrics.get(), 0);
    }

    @Test
    public void testCheckIfNearingExpiration() {
        AuthenticationProviderTls.checkIfNearingExpiration(chain[0], CLIENT_CN,
                1, 35000L);
        assertEquals(1, AuthenticationProviderTls.clientCertReachedWarnThresholdMetrics.get(), 0);
        AuthenticationProviderTls.checkIfNearingExpiration(chain[0], CLIENT_CN,
                35000L, 1L);
        assertEquals(1, AuthenticationProviderTls.clientCertReachedErrorThresholdMetrics.get(), 0);
    }

    @Test
    public void testCheckMaxValidityPeriod() {
        AuthenticationProviderTls.checkMaxValidityPeriod(chain[0], CLIENT_CN, 59999);
        assertEquals(1, AuthenticationProviderTls.clientCertValidityDurationExceedsThresholdMetrics.get(), 0);
    }

    @Test
    public void testCheckIfUsingSmallRsaKeySize() {
        AuthenticationProviderTls.checkIfUsingSmallRsaKeySize(chain[0], 4096, CLIENT_CN);
        assertEquals(1, AuthenticationProviderTls.clientCertSmallRsaKeySizeMetrics.get(), 0);
    }

    private X509Certificate[] generateCertificateChain(final String[][] chainCommonNames,
             final long millisBefore, final long millisAfter) throws NoSuchAlgorithmException,
            CertIOException, CertificateException, OperatorCreationException {

        X509Certificate[] certChain = new X509Certificate[chainCommonNames.length];

        X500Name[] certNames = new X500Name[chainCommonNames.length];
        BigInteger[] serialNumbers = new BigInteger[chainCommonNames.length];

        for(int i = 0; i< certChain.length; i++) {
            X500NameBuilder x500NameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
            x500NameBuilder.addRDN(BCStyle.CN, chainCommonNames[i][0]);
            x500NameBuilder.addRDN(BCStyle.OU, chainCommonNames[i][1]);
            x500NameBuilder.addRDN(BCStyle.O, chainCommonNames[i][2]);
            certNames[i] = x500NameBuilder.build();

            serialNumbers[i] = new BigInteger(UUID.randomUUID().toString().replace("-", ""), 16);
        }

        for(int i = 0; i< certChain.length; i++) {

            Instant now = Instant.now();

            Instant notBefore = now.minusMillis(millisBefore);
            Instant notAfter = now.plusMillis(millisAfter);


            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048);
            KeyPair key = keyGen.generateKeyPair();
            PrivateKey priv = key.getPrivate();
            PublicKey pub = key.getPublic();

            final byte[] encoded = pub.getEncoded();
            SubjectPublicKeyInfo subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(
                    ASN1Sequence.getInstance(encoded));

            final X509v3CertificateBuilder generator = new X509v3CertificateBuilder(
                    (i == 0) ? certNames[i] /* CA */: certNames[i-1] /* Not a CA */,
                    serialNumbers[i],
                    Date.from(notBefore),
                    Date.from(notAfter),
                    certNames[i],
                    subjectPublicKeyInfo);

            //generator.addExtension(Extension.certificateIssuer, true, new IssuerSerial());


            if(i != 0) {
                // Not a CA
                generator.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.dataEncipherment));
                generator.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
            } else {
                // CA
                generator.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
            }

            X509CertificateHolder holder = generator.build(
                    new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(priv));

            certChain[i] = new JcaX509CertificateConverter().getCertificate(holder);

        }

        Collections.reverse(Arrays.asList(certChain));
        return certChain;
    }
}
