package io.pereira.iceberglookupsrv.stats;

import org.apache.commons.codec.binary.Hex;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * TableStats contains stats for a table. Stats are only maintained for one column.
 *
 * @param tableName  name of the table
 * @param columnName column name of the stats
 * @param columnId   Iceberg column ID of the stats
 * @param columnType column type of the stats
 * @param splitStats file stats
 */
public record TableStats(
        String tableName,
        Schema tableSchema,
        String columnName,
        Integer columnId,
        Type columnType,
        List<SplitStats> splitStats
) {
    public void prettyPrint() {
        System.out.printf("table stats for table=%s column=%s (id=%s type=%s)\n", this.tableName, this.columnName, this.columnId, this.columnType);
        System.out.printf("total file count=%s\n", this.splitStats.size());

        for (SplitStats file : this.splitStats) {
            System.out.printf("\npath=%s\n", file.path());
            Object decodedLowerBound = decodeValue(this.columnType, file.lowerValue().bytes());
            Object decodedUpperBound = decodeValue(this.columnType, file.upperValue().bytes());
            System.out.printf("range=[%s, %s]\n", decodedLowerBound, decodedUpperBound);
        }
    }


    /**
     * Decodes the column value for pretty printing.
     *
     * @param value to decide
     * @return decoded value
     */
    private static Object decodeValue(Type columnType, ByteBuffer value) {
        // When the column is a binary type, encode it as a hex string for readability.
        if (columnType.equals(Types.BinaryType.get())) {
            return Hex.encodeHexString(value);
        }
        return Conversions.fromByteBuffer(columnType, value);
    }
}
