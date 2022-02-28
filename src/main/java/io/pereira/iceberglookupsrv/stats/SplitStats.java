package io.pereira.iceberglookupsrv.stats;

import io.pereira.iceberglookupsrv.algo.Range;

import java.nio.ByteBuffer;

/**
 * FileStats contains file stats for a file. Stats pertain to a column specified in the parent {@link TableStats}
 * <p>
 * TODO(lgo): Determine if using {@code byte[]} would be more memory efficient than {@link ByteBuffer}
 * <p>
 * TODO(lgo): Consider refactoring this to a {@code SplitStats} which can address a Parquet block rather than just a file.
 */
public class SplitStats implements Range<ComparableUnsignedBytes> {

    private final String path;
    private final ComparableUnsignedBytes lowerValue;
    private final ComparableUnsignedBytes upperValue;
    private final Long start;
    private final Long length;

    /**
     * Creates FileStats from the Iceberg metadata info.
     *
     * @param path       file path
     * @param lowerValue lower value contained in the file
     * @param upperValue upper value contained in the file
     */
    SplitStats(String path, ByteBuffer lowerValue, ByteBuffer upperValue) {
        this.path = path;
        this.lowerValue = ComparableUnsignedBytes.wrap(lowerValue);
        this.upperValue = ComparableUnsignedBytes.wrap(upperValue);
        this.start = null;
        this.length = null;
    }

    /**
     * Creates FileStats from the Iceberg metadata info.
     *
     * @param path       file path
     * @param lowerValue lower value contained in the file
     * @param upperValue upper value contained in the file
     */
    SplitStats(String path, ByteBuffer lowerValue, ByteBuffer upperValue, Long start, Long length) {
        this.path = path;
        this.lowerValue = ComparableUnsignedBytes.wrap(lowerValue);
        this.upperValue = ComparableUnsignedBytes.wrap(upperValue);
        this.start = start;
        this.length = length;
    }

    /**
     * Creates FileStats from the Iceberg metadata info.
     *
     * @param path       file path
     * @param lowerValue lower value contained in the file
     * @param upperValue upper value contained in the file
     */
    private SplitStats(String path, ComparableUnsignedBytes lowerValue, ComparableUnsignedBytes upperValue, Long start, Long length) {
        this.path = path;
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        this.start = start;
        this.length = length;
    }


    public String path() {
        return path;
    }

    @Override
    public ComparableUnsignedBytes lowerValue() {
        return lowerValue;
    }

    @Override
    public ComparableUnsignedBytes upperValue() {
        return upperValue;
    }

    public Long getStart() {
        return start;
    }

    public Long getLength() {
        return length;
    }

    public boolean isBlockLevel() {
        return this.start != null;
    }
}
