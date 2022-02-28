package io.pereira.iceberglookupsrv.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.hamcrest.core.StringContains;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

public class IcebergUtilTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    /**
     * Warehouse path for all tests. Populated during {@link #setUp()}.
     */
    String warehousePath;

    final int ID_COLUMN_ID = 1;

    final Schema SCHEMA_ID_INT = new Schema(
            required(ID_COLUMN_ID, "id", IntegerType.get())
    );

    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration();
        File warehouseFolder = folder.newFolder("warehouse");
        warehousePath = warehouseFolder.getAbsolutePath();
    }

    /**
     * Creates an {@link Table} in the test warehouse.
     *
     * @param tableName of the table to create
     * @param schema    to set for the table
     * @return Iceberg table
     * @throws IOException
     */
    private Table createTable(String tableName, Schema schema) throws IOException {
        Configuration conf = new Configuration();
        try (HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath)) {
            return catalog.createTable(TableIdentifier.parse(tableName), schema);
        }
    }


    /**
     * Creates a {@link DataFile} assuming the column stats are for {@link #ID_COLUMN_ID}.
     *
     * @param fileName of the file
     * @param lower    for the ID column
     * @param upper    for the ID column
     * @return the datafile
     * @see #createDataFile(String, Integer, Integer)
     */
    private DataFile createDataFile(String fileName, Integer lower, Integer upper) {
        return createDataFile(fileName, ID_COLUMN_ID, lower, upper);
    }

    /**
     * Creates a {@link DataFile} with a filename and metrics for the columnId, including the provided lowerValue/upperValue values.
     * <p>
     * Note, all other properties about the table use placeholder values.
     *
     * @param fileName of the file
     * @param columnId for the metrics
     * @param lower    for the column
     * @param upper    for the column
     * @return the datafile
     */
    private DataFile createDataFile(String fileName, Integer columnId, Integer lower, Integer upper) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(fileName)
                .withFileSizeInBytes(1234L)
                .withRecordCount(1234L)
                .withMetrics(new Metrics(
                        1234L,
                        Map.of(columnId, 1234L),
                        Map.of(columnId, 1234L),
                        Map.of(columnId, 1234L),
                        Map.of(columnId, 1234L),
                        Map.of(columnId, Conversions.toByteBuffer(Types.IntegerType.get(), lower)),
                        Map.of(columnId, Conversions.toByteBuffer(Types.IntegerType.get(), upper))
                ))
                .build();
    }


    @Test
    public void testLoader_works_withEmptyTable() throws IOException, IcebergUtil.IcebergError {
        // Create table.
        createTable("foobar", SCHEMA_ID_INT);
        TableStats stats = IcebergUtil.loadTableStats(warehousePath, "foobar", "id");

        assertEquals(stats.tableName(), "foobar");
        assertEquals(stats.columnName(), "id");
        assertEquals(stats.columnId(), (Integer) 1);
        assertEquals(stats.columnType(), IntegerType.get());

        assertEquals(stats.splitStats().size(), 0);
    }

    @Test
    public void testLoader_works() throws IOException, IcebergUtil.IcebergError {
        // Create table.
        Table table = createTable("foobar", SCHEMA_ID_INT);
        // Create data file.
        table.newOverwrite().addFile(createDataFile("zaz.parquet", 10, 20)).commit();

        TableStats stats = IcebergUtil.loadTableStats(warehousePath, "foobar", "id");

        // Assert the table info is correct.
        assertEquals(stats.tableName(), "foobar");
        assertEquals(stats.columnName(), "id");
        assertEquals(stats.columnId(), (Integer) 1);
        assertEquals(stats.columnType(), IntegerType.get());

        // Assert there are stats for one file.
        assertEquals(stats.splitStats().size(), 1);

        // Assert the correctness of the file's stats.
        SplitStats fileStat = stats.splitStats().get(0);
        assertEquals(fileStat.path(), "zaz.parquet");
        assertEquals(Conversions.fromByteBuffer(Types.IntegerType.get(), fileStat.lowerValue().bytes()), (Integer) 10);
        assertEquals(Conversions.fromByteBuffer(Types.IntegerType.get(), fileStat.upperValue().bytes()), (Integer) 20);
    }

    @Test
    public void testLoader_raises_withMissingColumnStats() throws IOException, IcebergUtil.IcebergError {
        // Create table.
        Table table = createTable("foobar", SCHEMA_ID_INT);
        // Create data file. Use a non-default column ID for metrics so no metrics are found for the specified column.
        table.newOverwrite().addFile(createDataFile("zaz.parquet", 2, 10, 20)).commit();

        exceptionRule.expect(IcebergUtil.DatafileMissingMetricsError.class);
        exceptionRule.expectMessage(StringContains.containsString("data file is missing metrics"));
        TableStats stats = IcebergUtil.loadTableStats(warehousePath, "foobar", "id");
    }

    @Test
    public void testLoader_raises_withNoStats() throws IOException, IcebergUtil.IcebergError {
        // Create table.
        Table table = createTable("foobar", SCHEMA_ID_INT);
        // Create data file.
        DataFile dataFile = createDataFile("zaz.parquet", 10, 20);
        // Strip the stats off the data file before committing it.
        DataFile dataFileWithoutStats = dataFile.copyWithoutStats();
        table.newOverwrite().addFile(dataFileWithoutStats).commit();

        exceptionRule.expect(IcebergUtil.DatafileMissingMetricsError.class);
        exceptionRule.expectMessage(StringContains.containsString("data file is missing metrics"));
        TableStats stats = IcebergUtil.loadTableStats(warehousePath, "foobar", "id");
    }

    @Test
    public void testLoader_raises_withUnsupportedType() throws IOException, IcebergUtil.IcebergError {
        Schema unsupportedSchema = new Schema(
                required(1, "id", Types.StructType.of(Types.NestedField.of(
                        2, false, "nested", IntegerType.get()
                )))
        );
        // Create table.
        Table table = createTable("foobar", unsupportedSchema);

        exceptionRule.expect(IcebergUtil.ColumnTypeUnsupportedError.class);
        exceptionRule.expectMessage(StringContains.containsString("column type unsupported as only primitive types are supported"));
        TableStats stats = IcebergUtil.loadTableStats(warehousePath, "foobar", "id");
    }

    @Test
    public void testLoader_raises_withMissingColumn() throws IOException, IcebergUtil.IcebergError {
        // Create table.
        Table table = createTable("foobar", SCHEMA_ID_INT);

        exceptionRule.expect(IcebergUtil.NoSuchColumnFound.class);
        exceptionRule.expectMessage(StringContains.containsString("column not_a_column found on table."));
        TableStats stats = IcebergUtil.loadTableStats(warehousePath, "foobar", "not_a_column");
    }

    @Test
    public void testLoader_raises_withUnaccessibleCatalog() throws IcebergUtil.IcebergError {
        exceptionRule.expect(NoSuchNamespaceException.class);
        exceptionRule.expectMessage(StringContains.containsString("Namespace does not exist"));
        TableStats stats = IcebergUtil.loadTableStats("/wow/okay", "foobar", "id");
    }

    @Test
    public void testLoader_raises_withNonexistentTable() throws IcebergUtil.IcebergError, IOException {
        exceptionRule.expect(NoSuchTableException.class);
        exceptionRule.expectMessage(StringContains.containsString("Table does not exist"));
        TableStats stats = IcebergUtil.loadTableStats(warehousePath, "foobar", "id");
    }
}