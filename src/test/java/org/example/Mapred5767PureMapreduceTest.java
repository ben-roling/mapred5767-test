package org.example;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class Mapred5767PureMapreduceTest {

    @Test
    public void testMapred5767PureMapreduce() throws Exception {
        final File file1 = tempFolder.newFile("file1.avro");
        // ~30MB
        final StringList record1 = createRecord(true, 10000000);

        // write of the string in this big record will fall such that bufindex + len == bufstart exactly
        // (when io.sort.mb = default of 100)
        final StringList record2 = createRecord(false, 99415882);

        // adding some more data to the big record so there is an additional write
        // after the write where bufindex + len == bufstart
        record2.getValue().add("helloworld!!!!!!!!!!!!!");

        writeFile(file1, record1, record2);

        final Configuration conf = new Configuration();
        conf.set("io.sort.mb", "100");
        final Job job = new Job(conf);

        final File outputFolder = tempFolder.newFolder();
        outputFolder.delete();
        FileInputFormat.addInputPath(job, new Path(file1.getAbsolutePath()));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder.getAbsolutePath()));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(MyMapper.class);
        AvroJob.setInputKeySchema(job, StringList.SCHEMA$);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Type.INT));
        AvroJob.setMapOutputValueSchema(job, StringList.SCHEMA$);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(MyReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Type.INT));
        AvroJob.setOutputValueSchema(job, StringList.SCHEMA$);

        job.waitForCompletion(true);

        assertTrue(job.isSuccessful());
    }

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    public static class MyMapper extends
            Mapper<AvroKey<StringList>, NullWritable, AvroKey<Integer>, AvroValue<StringList>> {

        @Override
        public void map(final AvroKey<StringList> key, final NullWritable value, final Context context)
                throws IOException, InterruptedException {

            context.write(new AvroKey<Integer>(key.datum().getValue().size()), new AvroValue<StringList>(key.datum()));
        }
    }

    public static class MyReducer extends
            Reducer<AvroKey<Integer>, AvroValue<StringList>, AvroKey<Integer>, AvroValue<StringList>> {

        @Override
        public void reduce(final AvroKey<Integer> key, final Iterable<AvroValue<StringList>> values,
                final Context context) throws IOException, InterruptedException {

            for (final AvroValue<StringList> value : values) {
                context.write(key, value);
            }
        }
    }

    private static StringList createRecord(final boolean isEven, final int count) throws IOException {
        final List<CharSequence> value = new ArrayList<CharSequence>();
        final int elementMaxSize = 1000;
        for (int i = 0; i <= count / elementMaxSize; i++) {
            final StringBuffer buf = new StringBuffer(elementMaxSize);
            for (int j = i * elementMaxSize; (j < (i + 1) * elementMaxSize) && (j < count); j++) {
                // isEven 0 2 4 6 8 0 2 4 6 8 ...
                // else 1 3 5 7 9 1 3 5 7 9 ...
                buf.append(Integer.toString((j * 2 + (isEven ? 0 : 1)) % 10));
            }
            if (buf.toString().length() > 0) {
                value.add(buf.toString());
            }
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
