package io.pereira.iceberglookupsrv.algo;

import java.util.ArrayList;
import java.util.List;

/**
 * ListIndex provides a naive implementation of an index over a list of ranges with no sorting or search algorithms employed.
 *
 * @param <T>
 */
public class ListIndex<T extends Comparable<T>> implements RangeSearchIndex<T> {

    List<Range<T>> ranges;

    public ListIndex(List<Range<T>> ranges) {
        this.ranges = ranges;
    }

    @Override
    public List<Range<T>> findMatchingRanges(T value) {
        List<Range<T>> matching = new ArrayList<>();
        for (Range<T> range : ranges) {
            if (range.contains(value)) {
                matching.add(range);
            }
        }
        return matching;
    }
}
