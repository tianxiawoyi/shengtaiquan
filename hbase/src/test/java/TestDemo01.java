import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestDemo01 {


    private Table myuser;
    private Connection connection;
    private Configuration configuration;

    @Test
    public void createTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01,node02,node03");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
       //configuration.set("hbase.master", "node01:16000");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("myuser"));
        hTableDescriptor.addFamily(new HColumnDescriptor("f1"));
        hTableDescriptor.addFamily(new HColumnDescriptor("f2"));
        admin.createTable(hTableDescriptor);

        admin.close();
        connection.close();

    }

    @Test
    public void addData() throws IOException {

        Put put = new Put("rkwz0003".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("张三"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("深圳"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("13450145275"));

        Put put2 = new Put("rkwz0002".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("李四"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(14));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("深圳"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("13450145235"));

        myuser.put(Arrays.asList(put,put2));
        myuser.close();
    }


    @Before
    public void init() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");

        connection = ConnectionFactory.createConnection(configuration);
        myuser = connection.getTable(TableName.valueOf("myuser"));
    }

    @After
    public void destroy() throws IOException {
        if(!connection.isClosed()){
            connection.close();
        }
        myuser.close();

    }

    @Test
    public void insertBatchData() throws IOException {

        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表
        Table myuser = connection.getTable(TableName.valueOf("myuser"));
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(30));
        put.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));


        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<Put>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        myuser.put(listPut);
        myuser.close();
    }

    @Test
    public void getData() throws IOException {
        Get get = new Get("rkwz0002".getBytes());
        //get.addColumn("f1".getBytes(),"id".getBytes());  //获取列族名为f1,列明为id的所有
        Result result = myuser.get(get);

        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            //获取列族的名称
            String familyName = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String familyName1 = Bytes.toString(cell.getFamilyArray());
            //获取列的名称
            String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            String columnName1 = Bytes.toString(cell.getQualifierArray());
            String ValueArray = Bytes.toString(cell.getValueArray());
            Object value;
            if(familyName.equals("f1") && columnName.equals("id")||columnName.equals("age")){
                 value = Bytes.toInt(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            }else{
                 value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            }
            System.out.println("列族名为:"+familyName+"     列名为:" +  columnName + "     列的值为:" +  value);

        }

    }

    @Test
    public void scanRange() throws IOException {
        Scan scan = new Scan();

         scan.setStartRow("0004".getBytes());
        scan.setStopRow("0006".getBytes());

        ResultScanner scanner = myuser.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                //获取列族的名称
                String familyName = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String familyName1 = Bytes.toString(cell.getFamilyArray());
                //获取列的名称
                String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String columnName1 = Bytes.toString(cell.getQualifierArray());
                String ValueArray = Bytes.toString(cell.getValueArray());
                Object value;
                if(familyName.equals("f1") && columnName.equals("id")||columnName.equals("age")){
                    value = Bytes.toInt(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                }else{
                    value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                }
                System.out.println("列族名为:"+familyName+"     列名为:" +  columnName + "     列的值为:" +  value);

            }

        }

    }


    @Test
    public void filterStudy() throws IOException {

        Scan scan = new Scan();
        Filter filter =null;
        //查询rowkey比0003小的所有的数据
        // filter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("0003")));

        //查询比f2列族小的所有的列族里面的数据
        // filter = new FamilyFilter(CompareFilter.CompareOp.LESS, new SubstringComparator("f2"));

        //只查询name列的值
       // filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("name") );

        //查询value值当中包含8的所有的数据(值过滤器)
        filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("8"));

        //查询name值为刘备的数据 select * from myuser where name ="刘备"
       // filter= new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());

        //查询rowkey以00开头所有的数据
        //filter = new PrefixFilter("rkwz".getBytes());

        scan.setFilter(filter);

                //需求：使用SingleColumnValueFilter查询f1列族，name为刘备的数据，并且同时满足rowkey的前缀以00开头的数据（PrefixFilter）
                 Filter prefixFilter =  new PrefixFilter("00".getBytes());
                 SingleColumnValueFilter  filter2 = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
                //使用filterList来实现多过滤器综合查询
                FilterList filterList = new FilterList(filter2, prefixFilter);
                 scan.setFilter(filterList);
        ResultScanner scanner = myuser.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                //获取列族的名称
                String familyName = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String familyName1 = Bytes.toString(cell.getFamilyArray());
                //获取列的名称
                String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String columnName1 = Bytes.toString(cell.getQualifierArray());
                String ValueArray = Bytes.toString(cell.getValueArray());
                Object value;
                if(familyName.equals("f1") && columnName.equals("id")||columnName.equals("age")){
                    value = Bytes.toInt(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                }else{
                    value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                }
                System.out.println("rowkey为:" +  rowkey+"    列族名为:"+familyName+"     列名为:" +  columnName + "     列的值为:" +  value);

            }

        }

    }

}
