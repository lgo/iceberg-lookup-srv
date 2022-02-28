package io.pereira.iceberglookupsrv;

import io.pereira.iceberglookupsrv.algo.BinarySearchListIndex;
import io.pereira.iceberglookupsrv.algo.Range;
import io.pereira.iceberglookupsrv.algo.RangeSearchIndex;
import io.pereira.iceberglookupsrv.stats.ComparableUnsignedBytes;
import io.pereira.iceberglookupsrv.stats.IcebergUtil;
import io.pereira.iceberglookupsrv.stats.SplitStats;
import io.pereira.iceberglookupsrv.stats.TableStats;
import org.apache.avro.generic.GenericData;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Service {
    private static final Logger LOGGER = LogManager.getLogger(Service.class.getName());

    public static void main(String[] args) throws IcebergUtil.IcebergError, IOException {
        String warehousePath = args[0];
        String tableName = args[1];
        String columnName = args[2];

        LOGGER.info(String.format("executing with warehouse=%s table=%s column=%s", warehousePath, tableName, columnName));

        // Load stats.
        TableStats stats = IcebergUtil.loadTableStats(warehousePath, tableName, columnName);
        LOGGER.info(String.format("Loaded stats for table. file_len=%d", stats.splitStats().size()));
        stats.prettyPrint();

        // Create indexes.
        List<Range<ComparableUnsignedBytes>> ranges = stats.splitStats().stream().map(f -> (Range<ComparableUnsignedBytes>) f).toList();
        RangeSearchIndex<ComparableUnsignedBytes> index = new BinarySearchListIndex<ComparableUnsignedBytes>(ranges);

        // Hardcoded search value.
        ComparableUnsignedBytes value = ComparableUnsignedBytes.wrap(idDigest("1"));
        String hex = Hex.encodeHexString(value.bytes());
        LOGGER.info(String.format("starting search for %s", hex));

        // Query index.
        List<SplitStats> matchingFiles = index.findMatchingRanges(value).stream().map(r -> (SplitStats) r).toList();
        LOGGER.info(String.format("found %d matching files", matchingFiles.size()));

        // Pull rows from files.
        List<GenericData.Record> results = new ArrayList<>();
        for (SplitStats f : matchingFiles) {
            results.addAll(IcebergUtil.scanFileForRows(stats.tableSchema(), f, columnName, value));
        }
        LOGGER.info(String.format("found %d results", results.size()));

        // Pull block metadata.
        TableStats blockTableStats = IcebergUtil.expandAsBlockStats(stats);

        // FIXME(joey): Refactor this out so:
        //  - We have a loadFileStats and loadBlockStats
        //  - Both stats inherit the same abstract class for ranging/indexing
        //  - We can construct indexes that are block or file level
        //    - Ideally make it a bool to just select one of the two. Yay!
        //  - We can initiate a scan from either stats file -> implies it implements a "readable" method
        //    - Ideally it is implemented so that we do not reload Iceberg metadata but can initial a file scan. Maybe we avoid closing it?
        //    - Or just use custom implementation of Parquet readers, like below...
        for (SplitStats block : blockTableStats.splitStats()) {
            LOGGER.info(String.format("row group startPos=%d len=%d file=%s", block.getStart(), block.getLength(), block.path()));
            LOGGER.info(String.format("stats: [%s, %s]", Hex.encodeHexString(block.lowerValue().bytes()), Hex.encodeHexString(block.upperValue().bytes())));
        }

        // OUTCOME: Splits == the same block metadata.
        IcebergUtil.logSplits(warehousePath, tableName);
    }

    private final static int ID_DIGEST_LEN = 12;

    private static byte[] idDigest(String v) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] hash = digest.digest(v.getBytes(StandardCharsets.UTF_8));
        return Arrays.copyOfRange(hash, 0, ID_DIGEST_LEN);
    }
}
