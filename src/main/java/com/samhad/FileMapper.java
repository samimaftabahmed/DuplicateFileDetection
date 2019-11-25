package com.samhad;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FileMapper extends Mapper<Object, Text, Text, BytesWritable> {


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {


    }
}
