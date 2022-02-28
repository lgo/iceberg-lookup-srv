/**
 * Source from https://github.com/saurabhagas/iceberg-generator
 */

package io.pereira.iceberglookupsrv.iceberg.util;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.After;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TableTestBase {
    protected File tableDir = null;
    protected TestTables.TestTable table = null;

    @After
    public void cleanupTables() {
        TestTables.clearTables();
    }

    protected TestTables.TestTable create(Schema schema, PartitionSpec spec) {
        requireNonNull(tableDir, "tableDir not set in the test");
        return TestTables.create(tableDir, "test", schema, spec);
    }

    protected Set<String> pathSet(DataFile... files) {
        return Sets.newHashSet(Iterables.transform(Arrays.asList(files), file -> file.path().toString()));
    }

    protected Set<String> pathSet(Iterable<DataFile> files) {
        return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
    }
}