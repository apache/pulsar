package org.apache.pulsar.protocols.grpc;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class HmacSigner {

    private static final String HMAC_SHA256 = "HmacSHA256";
    private SecretKeySpec key;

    /**
     * Creates a {@link HmacSigner} instance using a randomly generated key.
     */
    public HmacSigner() {
        byte[] secret = new byte[32];
        new SecureRandom().nextBytes(secret);
        this.key = new SecretKeySpec(secret, HMAC_SHA256);
    }

    /**
     * Creates a {@link HmacSigner} instance using the specified key.
     *
     * @param key key to use for creating the HMAC.
     */
    public HmacSigner(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("secret cannot be NULL");
        }
        this.key = new SecretKeySpec(key.clone(), HMAC_SHA256);
    }

    /**
     * Returns the signature of a byte array.
     *
     * @param data data to sign.
     * @return the signature for the data.
     */
    public byte[] computeSignature(byte[] data) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(key);
            return mac.doFinal(data);
        } catch (NoSuchAlgorithmException | InvalidKeyException ex) {
            throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
        }
    }
}
