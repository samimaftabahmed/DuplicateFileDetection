package com.samhad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

public class BinaryFilesToHadoopSequenceFile extends Mapper<Object, Text, Text, BytesWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String uri = value.toString();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;

        try {
            in = fs.open(new Path(uri));
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024 * 1024];

            while (in.read(buffer, 0, buffer.length) >= 0) {
                bout.write(buffer);
            }

            context.write(value, new BytesWritable(bout.toByteArray()));

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            IOUtils.closeStream(in);
        }

    }
}
