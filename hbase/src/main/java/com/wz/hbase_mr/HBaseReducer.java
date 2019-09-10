package com.wz.hbase_mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Text  key2的类型
 * Put   value2类型
 * ImmutableBytesWritable   k3的类型
 * V3的类型？？？
 * put 'myuser2','rowkey','f1:name','zhangsan'
 * javaAPI来写通过put对象即可
 *
 */
public class HBaseReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
    /**
     *
     * @param key  就是我们的key2
     * @param values  就是我们的v2
     * @param context  将我们的数据往外写出去
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put : values) {
            context.write(new ImmutableBytesWritable(key.toString().getBytes()),put);
        }
    }
}

