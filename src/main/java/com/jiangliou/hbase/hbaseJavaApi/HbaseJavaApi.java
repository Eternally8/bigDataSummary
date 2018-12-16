package com.jiangliou.hbase.hbaseJavaApi;

import com.jiangliou.dto.RowDatas;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ ClassName    :  HbaseJavaApi
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/16
 */
public class HbaseJavaApi {
    /**
     * @ Author jlo
     * @ Description 获取带有Hbase配置的Configuration
     * @ Date 12:41 2018/12/16
     **/
    public static Configuration conf = HBaseConfiguration.create();

    public static void main(String[] args) throws IOException {
        createTable("hbase_school","grade1","grade2","grade3");
        addRowData("hbase_school","school1_212321312","grade1","一年级","1(1)班");

        RowDatas row1 = new RowDatas("school1_212321313","grade1","一年级","1(2)班");
        RowDatas row2 = new RowDatas("school1_212321314","grade1","一年级","1(3)班");
        List<RowDatas> rowDatas = new ArrayList<>();
        rowDatas.add(row1);
        rowDatas.add(row2);
        addRowsData("hbase_school",rowDatas);

//        getRow("hbase_school","school1_212321312","grade1");

       getAllRows("hbase_school");
//        deleteRow("hbase_school","school1_212321312");
//    deleteColumn("hbase_school","school1_212321312","grade1","一年级");
        //deleteColumnFamily("hbase_school","grade1");
//        dropTable("hbase_school");
    }
    
    /**
     * @ Author jlo
     * @ Description 获取某一行指定“列族:列”的数据
     * @ Date 18:04 2018/12/16
     * @ Param [tableName, rowKey, cf, c]
     * @ return void
     **/
    public static void getRowQualifier(String tableName, String rowKey, String cf, String c) throws IOException { 
        Connection conn =null;
        Table table =null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c));
            Result result = table.get(get);
            for(Cell cell : result.rawCells()){
                System.out.println("行键:" + Bytes.toString(result.getRow()));
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
            conn.close();
        }
    }


    /**
     * @ Author jlo
     * @ Description 删除表
     * @ Date 17:57 2018/12/16
     * @ Param [tableName]
     * @ return void
     **/
    private static void dropTable(String tableName) throws IOException {
        Connection conn =null;
        Admin admin =null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e1) {
            e1.printStackTrace();
        }finally {
            admin.close();
            conn.close();
        }
    }

    /**
     * @ Author jlo
     * @ Description 删除列族
     * @ Date 17:47 2018/12/16
     * @ Param [tableName, cf]
     * @ return void
     **/
    private static void deleteColumnFamily(String tableName,String cf) throws IOException {
        Connection conn =null;
        Admin admin =null;
        try {
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
        admin.deleteColumn(TableName.valueOf(tableName),Bytes.toBytes(cf));
        } catch (IOException e1) {
            e1.printStackTrace();
        }finally {
            admin.close();
            conn.close();
        }
    }

    /**
     * @ Author jlo
     * @ Description 删除列
     * @ Date 17:07 2018/12/16
     * @ Param [tableName, rowkey, cf, c]
     * @ return void
     **/
    private static void deleteColumn(String tableName,String rowkey, String cf, String c) throws IOException {
        deleteColumn(tableName,rowkey,cf,c, -1);

    }
    /**
     * @ Author jlo
     * @ Description 删除列
     * @ Date 17:07 2018/12/16
     * @ Param [tableName, rowkey, cf, c, ts]
     * @ return void
     **/
    private static void deleteColumn(String tableName,String rowkey,String cf,String c,long ts) throws IOException {
        Connection conn =null;
        Table table =null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = null;
            if(-1 == ts ){
                delete = new Delete(Bytes.toBytes(rowkey));
                delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(c));
            }else{
                delete = new Delete(Bytes.toBytes(rowkey), ts);
                delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(c),ts);
            }
            table.delete(delete);
            System.out.println("delete column：" + rowkey + "," + cf + ":" + c + " success");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
            conn.close();
        }
    }


    /**
     * @ Author jlo
     * @ Description 删除行
     * @ Date 16:53 2018/12/16
     * @ Param [tableName, rowkey]
     * @ return void
     **/
    private static void deleteRow(String tableName,String rowkey) throws IOException {
        deleteRow(tableName,rowkey, -1);
    }

    /**
     * @ Author jlo
     * @ Description 删除行
     * @ Date 16:53 2018/12/16
     * @ Param [tableName, rowkey, ts]
     * @ return void
     **/
    private static void deleteRow(String tableName,String rowkey,long ts) throws IOException {
        Connection conn =null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Delete delete =null;
            if(-1 == ts){
                delete = new Delete(Bytes.toBytes(rowkey));
            }else {
                delete = new Delete(Bytes.toBytes(rowkey),ts);
            }
            table.delete(delete);
            System.out.println("delete row：" + rowkey + " success");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
            conn.close();
        }
    }

    /**
     * @ Author jlo
     * @ Description 扫描全表,展示多行
     * @ Date 16:45 2018/12/16
     * @ Param [tableName]
     * @ return void
     **/
    private static void getAllRows(String tableName) throws IOException {
        getAllRows(tableName,null,null);
    }
    /**
     * @ Author jlo
     * @ Description 扫描全表,展示多行
     * @ Date 16:43 2018/12/16
     * @ Param [tableName, startRow, stopRow]
     * @ return void
     **/
    private static void getAllRows(String tableName ,String startRow,String stopRow) throws IOException {
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            //创建扫描对象
            Scan scan = new Scan();
            if(null != startRow){
                scan.setStartRow(Bytes.toBytes(startRow));
            }
            if(null != stopRow){
                scan.setStopRow(Bytes.toBytes(stopRow));
            }
            //获取扫描对象
            ResultScanner scanner = table.getScanner(scan);
            long count = 0;
            System.out.println("查询结果：");
            for(Result result :scanner){
                count ++;
                printRow(result);
            }
            System.out.println(count + " row()");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
            conn.close();
        }
    }


    /**
     * @ Author jlo
     * @ Description 获取一行的数据
     * @ Date 16:10 2018/12/16
     * @ Param [tableName, rowkey]
     * @ return void
     **/
    private static void getRow(String tableName,String rowkey) throws IOException {
        getRow(tableName,rowkey,null,null, -1);
    }

    /**
     * @ Author jlo
     * @ Description 获取一行的数据
     * @ Date 16:10 2018/12/16
     * @ Param [tableName, rowkey, cf]
     * @ return void
     **/
    private static void getRow(String tableName,String rowkey, String cf) throws IOException {
        getRow(tableName,rowkey, cf,null, -1);
    }

    /**
     * @ Author jlo
     * @ Description 获取一行的数据
     * @ Date 16:10 2018/12/16
     * @ Param [tableName, rowkey, cf, c]
     * @ return void
     **/
    private static void getRow(String tableName,String rowkey, String cf, String c) throws IOException {
        getRow(tableName,rowkey, cf, c, -1);
    }

    /**
     * @ Author jlo
     * @ Description 获取一行的数据
     * @ Date 16:06 2018/12/16
     * @ Param [tableName, rowKey, cf, c, ts]
     * @ return void
     **/
    private static void getRow(String tableName,String rowKey,String cf,String c,long ts) throws IOException {
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            //创建Get对象
            Get get = new Get(Bytes.toBytes(rowKey));
            //显示所有的版本
            System.out.println(get.getMaxVersions());
            //显示指定时间戳的版本
            //System.out.println(get.setTimeStamp(323423423423L));
            //判空,将数据赋值进去
            if(null != cf){
                get.addFamily(Bytes.toBytes(cf));
            }
            if(null != c){
                get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(c));
            }
            if(ts != -1){
                get.setTimeStamp(ts);
            }
            Result result = table.get(get);
            System.out.println("查询结果为:");
            printRow(result);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(null != table){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if(null != conn){
                    conn.close();
                }
            }
        }
    }


    /**
     * @ Author jlo
     * @ Description 批量添加数据
     * @ Date 15:39 2018/12/16
     * @ Param [tableName, bean]
     * @ return void
     **/
    private static void addRowsData(String tableName,List<RowDatas> bean) throws IOException {
        //获取Hbase 连接
        Connection conn =null;
        Table table =null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            //循环添加信息
            List<Put> puts = new ArrayList<>();
            for(RowDatas datas : bean){
                Put put = new Put(Bytes.toBytes(datas.getRowKey()));
                if(datas.getTs() == -1){
                    put.addColumn(Bytes.toBytes(datas.getCf()),Bytes.toBytes(datas.getC()),Bytes.toBytes(datas.getValue()));
                }else {
                    put.addColumn(Bytes.toBytes(datas.getCf()),Bytes.toBytes(datas.getC()),datas.getTs(),Bytes.toBytes(datas.getValue()));
                }
                puts.add(put);
                System.out.println("put rowkey:" + datas.getRowKey() + " success!");
            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
            conn.close();
        }

    }


    /**
     * @ Author jlo
     * @ Description 向表中插入单行数据
     * @ Date 14:08 2018/12/16
     * @ Param [tableName, rowKey, cf, c, value]
     * @ return void
     **/
    private static void addRowData(String tableName,String rowKey,String cf,String c, String value) throws IOException {
         addRowData(tableName, rowKey, cf, c ,value,-1);
    }

    /**
     * @ Author jlo
     * @ Description 向表中插入单行数据(方法的重载)
     * @ Date 15:44 2018/12/16
     * @ Param [tableName, rowKey, cf, c, value, ts]
     * @ return void
     **/
    private static void addRowData(String tableName,String rowKey,String cf,String c, String value,long ts) throws IOException {
        //获取hbase连接
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取hbase的操作对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            //向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            //向Put对象中组装数据
            if(ts == -1){
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c), Bytes.toBytes(value));
            }else{
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c),ts, Bytes.toBytes(value));
            }
            //将数据插入到表中
            table.put(put);
            System.out.println("put rowkey:" + rowKey + " success!");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
            conn.close();
        }
    }

    /**
     * @ Author jlo
     * @ Description 创建表
     * @ Date 13:32 2018/12/16
     * @ Param [tableName, columnFamily]
     * @ return void
     **/
    public static void createTable(String tableName, String... columnFamily) {
        try (
                //获取Hbase的连接
                Connection conn = ConnectionFactory.createConnection(conf);
                //获取hbase的管理对象
                Admin admin = conn.getAdmin();
        ) {
            //判断表是否存在,如果存在就不创建
            if(admin.tableExists(TableName.valueOf(tableName))){
                System.out.println("tableName " + tableName.toString() + " exists!");
                return;
            }
            //创建表的描述
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            //循环添加列族
            for(String cf: columnFamily){
                table.addFamily(new HColumnDescriptor(cf));
            }
            //根据表的配置创建表
            admin.createTable(table);
            System.out.println("表" + tableName + "创建成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @ Author jlo
     * @ Description公共打印方法
     * @ Date 16:01 2018/12/16
     * @ Param
     * @ return
     **/
    private static void printRow(Result result) {
        Cell[] rawCells = result.rawCells();
        StringBuilder sb = new StringBuilder();
        for(Cell cell : rawCells){
            sb.append(new String(CellUtil.cloneRow(cell))).append("\tcolumn=").append(new String(CellUtil.cloneFamily(cell)))
                    .append(":").append(new String(CellUtil.cloneQualifier(cell))).append(",timestamp=")
                    .append(cell.getTimestamp()).append(", value=")
                    .append(new String(CellUtil.cloneValue(cell))).append("\n");
        }
        System.out.println(sb.toString());
    }
}
