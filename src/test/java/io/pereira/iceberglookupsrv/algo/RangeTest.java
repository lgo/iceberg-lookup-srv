package io.pereira.iceberglookupsrv.algo;

import junit.framework.TestCase;

public class RangeTest extends TestCase {
    public void testContains() {
        Range<Integer> r = new BasicRange(0, 2);

        assertTrue(r.contains(0));
        assertTrue(r.contains(1));
        assertTrue(r.contains(2));

        assertFalse(r.contains(-1));
        assertFalse(r.contains(3));
    }

    public void testCompareTo() {
        Range<Integer> r = new BasicRange(0, 2);

        assertEquals(r.compareTo(-1), 1);
        assertEquals(r.compareTo(0), 0);
        assertEquals(r.compareTo(1), 0);
        assertEquals(r.compareTo(2), 0);
        assertEquals(r.compareTo(3), -1);
    }
}