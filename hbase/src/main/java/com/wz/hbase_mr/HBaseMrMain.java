package com.wz.hbase_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseMrMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "hbaseMR");
        Scan scan = new Scan();
        //使用工具来来初始化我们的mapper类
        /**
         * String table, Scan scan,
         Class<? extends TableMapper> mapper,
         Class<?> outputKeyClass,
         Class<?> outputValueClass, Job job
         */
        TableMapReduceUtil.initTableMapperJob("myuser",scan,HBaseMapper.class, Text.class, Put.class,job);
        /**
         * String table,
         Class<? extends TableReducer> reducer, Job job
         */
        TableMapReduceUtil.initTableReducerJob("myuser2",HBaseReducer.class,job);

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181");
        int run = ToolRunner.run(configuration, new HBaseMrMain(), args);
        System.exit(run);
    }
}

