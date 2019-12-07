package com.jj.hhase.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * KeyIn map的输出就是rowkey
 * ValueIn map的输出就是put
 * KeyOut不需要输出
 */
public class CopyReducer extends TableReducer<ImmutableBytesWritable,Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for(Put put : values){
            context.write(NullWritable.get(),put);
        }
    }
}
