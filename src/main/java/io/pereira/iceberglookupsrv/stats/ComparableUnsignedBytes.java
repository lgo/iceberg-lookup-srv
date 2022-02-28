package io.pereira.iceberglookupsrv.stats;

import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * ComparableUnsignedBytes is a lightweight wrapper around a {@link ByteBuffer} to implement {@link Comparable} using the {@link UnsignedBytes} utils.
 * <p>
 * {@link ByteBuffer#compareTo(ByteBuffer)} implements a signed byte comparison, which will result in incorrect results such as due to the "out-of-bounds" short-circuit.
 */
public class ComparableUnsignedBytes implements Comparable<ComparableUnsignedBytes> {

    private final ByteBuffer bytes;

    private ComparableUnsignedBytes(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public static ComparableUnsignedBytes wrap(ByteBuffer bytes) {
        return new ComparableUnsignedBytes(bytes);
    }

    public static ComparableUnsignedBytes wrap(byte[] bytes) {
        return new ComparableUnsignedBytes(ByteBuffer.wrap(bytes));
    }

    public ByteBuffer bytes() {
        return this.bytes;
    }

    @Override
    public int compareTo(ComparableUnsignedBytes o) {
        return UnsignedBytes.lexicographicalComparator().compare(this.bytes.array(), o.bytes.array());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ComparableUnsignedBytes that = (ComparableUnsignedBytes) o;
        return Objects.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytes);
    }

    @Override
    public String toString() {
        return "ComparableUnsignedBytes{" +
                "bytes=" + Hex.encodeHexString(bytes) +
                '}';
    }
}
