package io.pereira.iceberglookupsrv.algo;

import java.util.List;

/**
 * Index provides an interface for range searching algorithms.
 *
 * @param <T>
 */
public interface RangeSearchIndex<T extends Comparable<T>> {

    /**
     * Finds all matching ranges for a value.
     *
     * @param value to search with
     * @return ranges that match the value
     */
    public List<Range<T>> findMatchingRanges(T value);
}
