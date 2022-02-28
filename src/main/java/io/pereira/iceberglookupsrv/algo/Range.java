package io.pereira.iceberglookupsrv.algo;

/**
 * Range represents a generic continuous range type that has a lower and upper value of the range.
 *
 * @param <T> a comparable type for the range
 */
public interface Range<T extends Comparable<T>> {

    /**
     * @return the lower value of the range
     */
    public T lowerValue();

    /**
     * @return the upper value of the range
     */
    public T upperValue();

    /**
     * @param value to check the range with
     * @return whether {@code value} is inside the range
     */
    public default boolean contains(T value) {
        return lowerValue().compareTo(value) <= 0 && upperValue().compareTo(value) >= 0;
    }

    /**
     * Compares the range to the provided value. Similar to a regular {@code compareTo} implementation, it returns:
     * <ul>
     *     <li>-1 if the {@code value} is above the range
     *     <li>0 if the range contains the {@code value}
     *     <li>1 if the {@code value} is below the range
     * </ul>
     *
     * @param value to compare with
     * @return the comparison result
     */
    public default int compareTo(T value) {
        if (lowerValue().compareTo(value) > 0) {
            return 1;
        } else if (upperValue().compareTo(value) < 0) {
            return -1;
        }
        return 0;
    }
}
