package io.pereira.iceberglookupsrv.algo;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests multiple {@link RangeSearchIndex} implementations to ensure they work.
 */
@RunWith(Parameterized.class)
public class RangeSearchIndexTest {

    private final IndexFactory.IRangeSearchIndexFactory indexFactory;

    @Parameterized.Parameters(name = "{index}: impl={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"ListIndex", new IndexFactory.ListIndexFactory()},
                {"BinarySearchListIndex", new IndexFactory.BinarySearchListIndexFactory()},
        });
    }

    public RangeSearchIndexTest(String ignoredName, IndexFactory.IRangeSearchIndexFactory indexFactory) {
        this.indexFactory = indexFactory;
    }

    /**
     * Tests that matching ranges works for non-overlapping ranges.
     */
    @Test
    public void testFindMatchingRanges_nonOverlappingRanges() {
        Range<Integer> lowerRange = new BasicRange(0, 1);
        Range<Integer> midRange = new BasicRange(2, 3);
        Range<Integer> upperRange = new BasicRange(10, 20);

        // Construct the list out of order to also test that indexes do not rely on index order.
        List<Range<Integer>> ranges = List.of(
                midRange,
                lowerRange,
                upperRange
        );

        RangeSearchIndex<Integer> index = indexFactory.build(ranges);

        // Check out-of-bounds
        assertEquals(index.findMatchingRanges(-1), List.of());
        assertEquals(index.findMatchingRanges(21), List.of());

        // Check a non-matching value between ranges
        assertEquals(index.findMatchingRanges(5), List.of());

        // Check matches.
        assertEquals(index.findMatchingRanges(0), List.of(lowerRange));
        assertEquals(index.findMatchingRanges(1), List.of(lowerRange));
        assertEquals(index.findMatchingRanges(3), List.of(midRange));
        assertEquals(index.findMatchingRanges(10), List.of(upperRange));
        assertEquals(index.findMatchingRanges(11), List.of(upperRange));
        assertEquals(index.findMatchingRanges(20), List.of(upperRange));
    }

    /**
     * Tests that matching ranges works for overlapping ranges. These can also have multiple results returned.
     */
    @Test
    public void testFindMatchingRanges_overlappingRanges() {
        List<Range<Integer>> ranges = List.of(
                new BasicRange(0, 3),
                new BasicRange(3, 3),
                new BasicRange(3, 3),
                new BasicRange(3, 5)
        );

        RangeSearchIndex<Integer> index = indexFactory.build(ranges);

        // Check out-of-bounds
        assertEquals(index.findMatchingRanges(-1), List.of());
        assertEquals(index.findMatchingRanges(6), List.of());

        // Check for a single match.
        assertEquals(index.findMatchingRanges(1), List.of(ranges.get(0)));

        // Check for multiple matches
        assertEquals(index.findMatchingRanges(3), List.of(
                ranges.get(0),
                ranges.get(1),
                ranges.get(2),
                ranges.get(3)
        ));
    }

    /**
     * Tests that matching ranges works for one range. Previously, there was a short-circuit that caused this path to break.
     */
    @Test
    public void testFindMatchingRanges_oneRange() {
        Range<Integer> range = new BasicRange(0, 2);

        RangeSearchIndex<Integer> index = indexFactory.build(List.of((range)));

        // Check out-of-bounds
        assertEquals(index.findMatchingRanges(-1), List.of());
        assertEquals(index.findMatchingRanges(3), List.of());

        // Check for a single match.
        assertEquals(index.findMatchingRanges(0), List.of(range));
        assertEquals(index.findMatchingRanges(1), List.of(range));
        assertEquals(index.findMatchingRanges(2), List.of(range));
        assertEquals(index.findMatchingRanges(1), List.of(range));
    }

    static ByteBuffer hexAsBuffer(String s) {
        try {
            return ByteBuffer.wrap(Hex.decodeHex(s));
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }
}