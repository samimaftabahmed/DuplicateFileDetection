package com.samhad;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FileMapper extends Mapper<Text, BytesWritable, Text, Text> {


    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

        String md5Str = DigestUtils.md5Hex(value.getBytes()).toUpperCase();

        Text text = new Text(md5Str);

        context.write(text, key);
    }
}
