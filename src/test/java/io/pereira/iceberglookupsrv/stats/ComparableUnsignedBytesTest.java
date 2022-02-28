package io.pereira.iceberglookupsrv.stats;

import junit.framework.TestCase;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class ComparableUnsignedBytesTest extends TestCase {

    public void testComparison() {
        assertTrue(fromHex("fffffe58464f9003b7753bbc").compareTo(fromHex("6b86b273ff34fce19d6b804e")) > 0);
        assertEquals(0, fromHex("fffffe58464f9003b7753bbc").compareTo(fromHex("fffffe58464f9003b7753bbc")));
        assertTrue(fromHex("fffffe58464f9003b7753bbc").compareTo(fromHex("fffffe58464f9003b7753bbb")) > 0);
        assertTrue(fromHex("fffffe58464f9003b7753bbc").compareTo(fromHex("fffffe58464f9003b7753bbd")) < 0);
    }


    static ComparableUnsignedBytes fromHex(String s) {
        try {
            return ComparableUnsignedBytes.wrap(Hex.decodeHex(s));
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }
}