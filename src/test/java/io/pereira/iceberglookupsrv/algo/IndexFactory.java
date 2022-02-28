package io.pereira.iceberglookupsrv.algo;

import java.util.List;

public class IndexFactory {
    /**
     * Provides an interface for test factories of RangeSearchIndex implementations, used for test cases to build {@code T} with input {@link Range}.
     *
     * @param <T> type to build.
     */
    public interface IRangeSearchIndexFactory {
        <T extends Comparable<T>> RangeSearchIndex<T> build(List<Range<T>> ranges);
    }

    public static class ListIndexFactory implements IRangeSearchIndexFactory {
        @Override
        public <T extends Comparable<T>> ListIndex<T> build(List<Range<T>> ranges) {
            return new ListIndex<T>(ranges);
        }
    }

    public static class BinarySearchListIndexFactory implements IRangeSearchIndexFactory {
        @Override
        public <T extends Comparable<T>> BinarySearchListIndex<T> build(List<Range<T>> ranges) {
            return new BinarySearchListIndex<T>(ranges);
        }
    }
}