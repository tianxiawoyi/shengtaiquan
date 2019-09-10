package com.wz.hbase_mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

public class HBaseMapper extends TableMapper<Text, Put> {
    /**
     * @param key  rowkey
     * @param value  封装了我们一行数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException, IOException {
        //  f1  name   age    f2   xxx
        //获取到我们的rowkey
        byte[] bytes = key.get();
        Put put = new Put(bytes);
        //获取Result当中所有的列
        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            //判断属于哪一个列族
            byte[] family = CellUtil.cloneFamily(cell);
            //获取cell属于哪一个列
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            if(Bytes.toString(family).equals("f1")){
                if(Bytes.toString(qualifier).equals("name") || Bytes.toString(qualifier).equals("age")){
                    put.add(cell);
                }
            }
        }
        if(!put.isEmpty()){
            context.write(new Text(Bytes.toString(bytes)),put);
        }
    }
}
