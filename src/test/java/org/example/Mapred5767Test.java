package org.example;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A test that demonstrates map output corruption caused by MAPRED-5767
 */
public class Mapred5767Test {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testMapred5767() throws IOException {
        final File file1 = tempFolder.newFile("file1.avro");
        // ~30MB
        final StringList record1 = createRecord(true, 10000000);

        // write of the string in this big record will fall such that bufindex + len == bufstart exactly
        // (when io.sort.mb = default of 100)
        final StringList record2 = createRecord(false, 99414881);

        // adding some more data to the big record so there is an additional write
        // after the write where bufindex + len == bufstart
        record2.getValue().add("helloworld!");

        writeFile(file1, record1, record2);

        // creating another file with a record to join to in Crunch -- the contents of this record don't really matter
        final File file2 = tempFolder.newFile("file2.avro");
        // ~30MB
        final StringList record3 = createRecord(true, 30000000);
        writeFile(file2, record3);

        final Configuration conf = new Configuration();
        // if you set this the test will pass
        // conf.set("io.sort.mb", "200");
        final MRPipeline pipeline = new MRPipeline(Mapred5767Test.class, conf);
        final PCollection<StringList> p1 = pipeline.read(new AvroFileSource<StringList>(new Path(file1
                .getAbsolutePath()), Avros.records(StringList.class)));
        final PCollection<StringList> p2 = pipeline.read(new AvroFileSource<StringList>(new Path(file2
                .getAbsolutePath()), Avros.records(StringList.class)));

        final MapFn<StringList, Pair<String, StringList>> firstValueMapFn = new FirstStringMapFn();

        final PTable<String, StringList> table1 = p1.parallelDo(firstValueMapFn,
                Avros.tableOf(Avros.strings(), Avros.records(StringList.class)));

        final PTable<String, StringList> table2 = p2.parallelDo(firstValueMapFn,
                Avros.tableOf(Avros.strings(), Avros.records(StringList.class)));

        final PTable<String, Pair<StringList, StringList>> joinResult = table1.join(table2);

        // this will result in ArrayIndexOutOfBoundsException due to corruption of the Map output due to MAPRED-5767
        // increase io.sort.mb and it runs without error
        assertNotNull(joinResult.asCollection().getValue());
    }

    private static StringList createRecord(final boolean isEven, final int count) throws IOException {
        final List<CharSequence> value = new ArrayList<CharSequence>();
        for (int i = 0; i <= count / 1000; i++) {
            final StringBuffer buf = new StringBuffer(1000);
            for (int j = i * 1000; (j < (i + 1) * 1000) && (j < count); j++) {
                // isEven 0 2 4 6 8 0 2 4 6 8 ...
                // else 1 3 5 7 9 1 3 5 7 9 ...
                buf.append(Integer.toString((j * 2 + (isEven ? 0 : 1)) % 10));
            }
            value.add(buf.toString());
        }
        return StringList.newBuilder().setValue(value).build();
    }

    private static void writeFile(final File file, final StringList... record) throws IOException {
        final DatumWriter<StringList> datumWriter = new GenericDatumWriter<StringList>(record[0].getSchema());
        final DataFileWriter<StringList> dataFileWriter = new DataFileWriter<StringList>(datumWriter);
        dataFileWriter.create(record[0].getSchema(), file);
        for (final StringList currentRecord : record) {
            dataFileWriter.append(currentRecord);
        }
        dataFileWriter.close();
    }
}
