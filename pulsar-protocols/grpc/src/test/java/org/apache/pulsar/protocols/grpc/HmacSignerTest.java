package org.apache.pulsar.protocols.grpc;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class HmacSignerTest {

    @Test
    public void testNoSecret() {
        assertThrows(IllegalArgumentException.class, () -> new HmacSigner(null));
    }

    @Test
    public void testComputeSignature() {
        HmacSigner signer = new HmacSigner("secret".getBytes());
        byte[] s1 = signer.computeSignature("ok".getBytes());
        byte[] s2 = signer.computeSignature("ok".getBytes());
        byte[] s3 = signer.computeSignature("wrong".getBytes());
        assertEquals(s1.length, 32);
        assertEquals(s1, s2);
        assertEquals(s3.length, 32);
        assertNotEquals(s1, s3);
    }

    @Test
    public void testGeneratedKey() {
        HmacSigner signer = new HmacSigner();
        byte[] s1 = signer.computeSignature("ok".getBytes());
        assertEquals(s1.length, 32);
    }

    @Test
    public void testNullAndEmptyString() {
        HmacSigner signer = new HmacSigner("secret".getBytes());
        byte[] s1 = signer.computeSignature(null);
        assertEquals(s1.length, 32);
        s1 = signer.computeSignature("".getBytes());
        assertEquals(s1.length, 32);
    }
}
