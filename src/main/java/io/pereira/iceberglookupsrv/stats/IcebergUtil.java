package io.pereira.iceberglookupsrv.stats;

import io.pereira.iceberglookupsrv.parquet.ParquetInputIO;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class IcebergUtil {
    private static final Logger LOGGER = LogManager.getLogger(IcebergUtil.class.getName());

    private static HadoopCatalog catalog(String warehousePath) {
        Configuration conf = new Configuration();
        return new HadoopCatalog(conf, warehousePath);
    }

    /**
     * Loads {@link TableStats} from an Iceberg table.
     * <p>
     * TODO(joey): Allow specifying a table name.
     * <p>
     * TODO(joey): Change warehousePath to a table loading abtraction.
     *
     * @param warehousePath Iceberg warehouse path
     * @param tableName     Iceberg table name
     * @param columnName    Iceberg column to keep stats for
     * @return TableStats containing stats for the specified column
     * @throws IcebergError if errors were encountered loading data
     */
    public static TableStats loadTableStats(String warehousePath, String tableName, String columnName) throws IcebergError {
        Instant start = Instant.now();

        List<SplitStats> splitStats = new ArrayList<>();

        Table table;
        try (HadoopCatalog catalog = catalog(warehousePath)) {
            List<String> namespaces = catalog.listNamespaces().stream().map(Namespace::toString).toList();
            table = catalog.loadTable(TableIdentifier.parse(tableName));
        } catch (IOException e) {
            throw new IcebergError("Exception while loading Iceberg catalog and table", e);
        }

        LOGGER.info(String.format("table location: %s", table.location()));

        Schema schema = table.schema();
        Integer columnId = schema.columns().stream().filter(c -> c.name().equals(columnName)).findFirst().map(Types.NestedField::fieldId).orElse(null);

        if (columnId == null) {
            List<String> validColumnNames = schema.columns().stream().map(Types.NestedField::name).toList();
            throw new NoSuchColumnFound(columnName, validColumnNames);
        }

        Type columnType = schema.findType(columnId);

        if (!columnType.isPrimitiveType()) {
            throw new ColumnTypeUnsupportedError(columnType);
        }

        try (CloseableIterable<FileScanTask> scanTasks = table.newScan().includeColumnStats().planFiles()) {
            scanTasks.iterator().forEachRemaining(task -> {
                DataFile file = task.file();

                Map<Integer, ByteBuffer> lowerBoundStats = file.lowerBounds();
                Map<Integer, ByteBuffer> upperBoundStats = file.upperBounds();

                if (lowerBoundStats == null || upperBoundStats == null) {
                    throw new DatafileMissingMetricsError(file);
                }

                ByteBuffer lowerBound = lowerBoundStats.get(columnId);
                ByteBuffer upperBound = upperBoundStats.get(columnId);

                if (lowerBound == null || upperBound == null) {
                    throw new DatafileMissingMetricsError(file);
                }

                splitStats.add(new SplitStats(
                        file.path().toString(),
                        lowerBound,
                        upperBound
                ));
            });
        } catch (IOException e) {
            throw new IcebergError("Exception while scanning Iceberg table file metadata", e);
        }

        Type type = schema.findType(columnId);

        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        LOGGER.info(String.format("loadTableStats executed in: %s", duration));

        return new TableStats(
                tableName,
                schema,
                columnName,
                columnId,
                columnType,
                splitStats
        );
    }

    public static TableStats expandAsBlockStats(TableStats stats) throws IOException {
        Instant startTime = Instant.now();

        ArrayList<SplitStats> blockStats = new ArrayList<>();

        for (SplitStats fileStat : stats.splitStats()) {
            LOGGER.info(String.format("loading row group metadata for file: %s", fileStat.path()));

            HadoopInputFile input = HadoopInputFile.fromLocation(fileStat.path(), new Configuration());
            List<BlockMetaData> rowGroups;
            try (ParquetFileReader schemaReader = ParquetFileReader.open(ParquetInputIO.file(input))) {
                rowGroups = schemaReader.getRowGroups();
            }
            for (BlockMetaData rowGroup : rowGroups) {
                ColumnChunkMetaData matchingColumn = getColumnStats(rowGroup, stats, fileStat);
                Statistics statistics = matchingColumn.getStatistics();
                ByteBuffer lower = ByteBuffer.wrap(statistics.getMinBytes());
                ByteBuffer upper = ByteBuffer.wrap(statistics.getMaxBytes());
                long start = rowGroup.getStartingPos();
                long length = rowGroup.getCompressedSize();

                blockStats.add(
                        new SplitStats(
                                fileStat.path(),
                                lower,
                                upper,
                                start,
                                length
                        )
                );
            }
        }

        Instant end = Instant.now();
        Duration duration = Duration.between(startTime, end);
        LOGGER.info(String.format("expandAsBlockStats executed in: %s", duration));

        return new TableStats(
                stats.tableName(),
                stats.tableSchema(),
                stats.columnName(),
                stats.columnId(),
                stats.columnType(),
                blockStats
        );
    }

    /**
     * Extracts column stats from a Parquet {@link BlockMetaData} for a column specified on {@link TableStats}.
     *
     * @param rowGroup
     * @param stats
     * @param fileStat
     * @return
     */
    private static ColumnChunkMetaData getColumnStats(BlockMetaData rowGroup, TableStats stats, SplitStats fileStat) {
        ColumnChunkMetaData matchingColumn = rowGroup.getColumns().stream().filter(c -> c.getPrimitiveType().getId().intValue() == stats.columnId()).findFirst().orElse(null);
        if (matchingColumn == null) {
            throw new ParquetFileColumnNotFound(stats.columnName(), fileStat.path());
        }
        return matchingColumn;
    }

    /**
     * Debugging tool to log the splits generated for an Iceberg table.
     * <p>
     * Used to verify the row group start+offsets match the Iceberg generated splits.
     *
     * @param warehousePath
     * @param tableName
     * @throws IOException
     */
    public static void logSplits(String warehousePath, String tableName) throws IOException {
        Table table;
        Configuration conf = new Configuration();
        try (HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath)) {
            List<String> namespaces = catalog.listNamespaces().stream().map(Namespace::toString).toList();
            table = catalog.loadTable(TableIdentifier.parse(tableName));
        } catch (IOException e) {
            throw new IcebergError("Exception while loading Iceberg catalog and table", e);
        }

        try (CloseableIterable<CombinedScanTask> scanTasks = table.newScan().includeColumnStats().planTasks()) {
            for (CombinedScanTask scanTask : scanTasks) {
                for (FileScanTask fileScanTask : scanTask.files()) {
                    LOGGER.info(String.format("scan task path=%s start=%d len=%d", fileScanTask.file().path(), fileScanTask.start(), fileScanTask.length()));
                }
            }
        }
    }

    /**
     * Scans a file and return rows where the column {@code columnName} equals {@code value}.
     *
     * @param schema     iceberg schema of the file
     * @param f          file to scan
     * @param columnName column to filter over
     * @param value      value to filter results by
     * @return
     * @throws IOException
     */
    public static Collection<GenericData.Record> scanFileForRows(Schema schema, SplitStats f, String columnName, ComparableUnsignedBytes value) throws IOException {
        Instant start = Instant.now();

        List<GenericData.Record> results = new ArrayList<>();
        // TODO(lgo) consider:
        //   1a. generating splits using Parquet.ReadBuilder#split(start, length)
        //   1b. parallelize the split reads
        //   2. indexing specific split stats. It is unclear how we can either (1) generate splits, or (2) read this data. Iceberg can probably generate splits for us?
        //   3. using parquet-mr reader to leverage column indexes. Iceberg currently does nt.
        HadoopInputFile input = HadoopInputFile.fromLocation(f.path(), new Configuration());
        Parquet.ReadBuilder builder = Parquet.read(input)
                .project(schema)
                .filter(Expressions.equal(columnName, value.bytes()))
                .filterRecords(true)
                .callInit();

        // TODO(joey): Apparently you can also push-down aggregations into parquet-mr according to Spark.
        try (CloseableIterable<GenericData.Record> records = builder.build()) {
            for (GenericData.Record record : records) {
                // TODO(joey): Return an iterable (or iterator?) from this, to prevent materializing all results in-memory.
                results.add(record);
            }
        }

        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        LOGGER.info(String.format("scanFileForRows executed in: %s", duration));

        return results;
    }

    public static class IcebergError extends RuntimeException {
        public IcebergError(String msg) {
            super(msg);
        }

        public IcebergError(String msg, Exception e) {
            super(msg, e);
        }
    }

    static class NoSuchColumnFound extends IcebergError {
        public NoSuchColumnFound(String columnName, List<String> validColumnNames) {
            super(String.format("column %s not found on table. available columns: %s", columnName, validColumnNames));
        }
    }

    static class ParquetFileColumnNotFound extends IcebergError {
        public ParquetFileColumnNotFound(String columnName, String filePath) {
            super(String.format("column %s not found in Parquet schema for file: %s", columnName, filePath));
        }
    }

    static class ColumnTypeUnsupportedError extends IcebergError {
        public ColumnTypeUnsupportedError(Type columnType) {
            super(String.format("column type unsupported as only primitive types are supported, instead got: %s", columnType));
        }
    }

    static class DatafileMissingMetricsError extends IcebergError {
        public DatafileMissingMetricsError(DataFile dataFile) {
            super(String.format("data file is missing metrics: %s", dataFile.path()));
        }
    }
}
