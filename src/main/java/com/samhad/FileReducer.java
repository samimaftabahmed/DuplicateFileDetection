package com.samhad;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FileReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Text imageTextPath = null;

        for (Text filePath : values) {
            imageTextPath = filePath;
            break;
        }

        context.write(imageTextPath, key);
    }
}
