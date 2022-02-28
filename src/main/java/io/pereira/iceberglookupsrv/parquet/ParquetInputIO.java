/**
 * Source from Iceberg.
 */

package io.pereira.iceberglookupsrv.parquet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Methods in this class translate from the IO API to Parquet's IO API.
 */
public class ParquetInputIO {
    private ParquetInputIO() {
    }

    public static InputFile file(org.apache.iceberg.io.InputFile file) {
        // TODO: use reflection to avoid depending on classes from iceberg-hadoop
        // TODO: use reflection to avoid depending on classes from hadoop
        if (file instanceof HadoopInputFile) {
            HadoopInputFile hfile = (HadoopInputFile) file;
            try {
                return org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(hfile.getStat(), hfile.getConf());
            } catch (IOException e) {
                throw new RuntimeIOException(e, "Failed to create Parquet input file for %s", file);
            }
        }
        return new ParquetInputFile(file);
    }

    static SeekableInputStream stream(org.apache.iceberg.io.SeekableInputStream stream) {
        if (stream instanceof DelegatingInputStream) {
            InputStream wrapped = ((DelegatingInputStream) stream).getDelegate();
            if (wrapped instanceof FSDataInputStream) {
                return HadoopStreams.wrap((FSDataInputStream) wrapped);
            }
        }
        return new ParquetInputStreamAdapter(stream);
    }

    private static class ParquetInputStreamAdapter extends DelegatingSeekableInputStream {
        private final org.apache.iceberg.io.SeekableInputStream delegate;

        private ParquetInputStreamAdapter(org.apache.iceberg.io.SeekableInputStream delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            delegate.seek(newPos);
        }
    }

    private static class ParquetInputFile implements InputFile {
        private final org.apache.iceberg.io.InputFile file;

        private ParquetInputFile(org.apache.iceberg.io.InputFile file) {
            this.file = file;
        }

        @Override
        public long getLength() throws IOException {
            return file.getLength();
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return stream(file.newStream());
        }
    }
}
