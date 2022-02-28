package io.pereira.iceberglookupsrv.algo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * BinarySearchListIndex provides an implementation of an index using a binary search list. Because the data is static, there is no requirement to implemented more advanced algorithms which serve to reduce insert/delete operation costs.
 * <p>
 * This implementation first sorts ranges then provides binary search lookups.
 *
 * @param <T>
 */
public class BinarySearchListIndex<T extends Comparable<T>> implements RangeSearchIndex<T> {

    List<Range<T>> sortedRanges;

    public BinarySearchListIndex(List<Range<T>> ranges) {
        this.sortedRanges = ranges.stream().sorted(
                Comparator.comparing(Range<T>::lowerValue).thenComparing(Range<T>::upperValue)
        ).toList();
    }

    @Override
    public List<Range<T>> findMatchingRanges(T value) {
        List<Range<T>> matching = new ArrayList<>();

        // Short-circuit: check if the value is outside the range bounds.
        boolean valueBelowLower = sortedRanges.get(0).lowerValue().compareTo(value) > 0;
        boolean valueAboveUpper = sortedRanges.get(sortedRanges.size() - 1).upperValue().compareTo(value) < 0;
        if (valueBelowLower || valueAboveUpper) {
            return matching;
        }

        // Find the index of a matching range.
        int matchIndex = findOneMatch(value);

        // A negative match indicates the value would belong inserted at index `-matchIndex`, but was not found.
        if (matchIndex < 0) {
            return matching;
        }

        // Include it in the matches.
        matching.add(sortedRanges.get(matchIndex));

        // Because this is a binary search, there may be additional matches before/after the index. These needed to both be checked. Searches starting from the match index can immediately be halted upon the first non-matching range.

        // Find all matches after the match index.
        for (int i = matchIndex + 1; i < sortedRanges.size(); i++) {
            Range<T> range = sortedRanges.get(i);
            if (!range.contains(value)) {
                break;
            }
            matching.add(range);
        }

        // Find any matches prior to the index.
        for (int i = matchIndex - 1; i >= 0; i--) {
            Range<T> range = sortedRanges.get(i);
            if (!range.contains(value)) {
                break;
            }
            matching.add(range);
        }

        return matching;
    }

    /**
     * findOneMatch will return the index of a single match for the value. Because of using binary search where multiple values may return, it is expected it can return any matching value at random.
     *
     * @param value to search
     * @return index in {@code sortedRanges} of a match
     */
    private int findOneMatch(T value) {
        return Collections.binarySearch(sortedRanges, value, new Comparator<Object>() {
            /**
             * Because the {@link Collections#binarySearch(List, Object, Comparator)} uses the comparator for comparing the array values against the provided value, we can leverage it to instead implement our range search.
             *
             * @param rangeObject the {@link Range<T>} being checked.
             * @param valueObject the {@link T} value object to be compared.
             * @return the comparison result of
             */
            @Override
            public int compare(Object rangeObject, Object valueObject) {
                Range<T> range = (Range<T>) rangeObject;
                T v = (T) valueObject;
                return range.compareTo(v);
            }
        });
    }
}
