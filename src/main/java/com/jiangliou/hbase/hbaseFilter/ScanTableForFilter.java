package com.jiangliou.hbase.hbaseFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ ClassName    :  ScanTableForFilter
 * @ Description  :  通过在scan里面添加filter来过滤条件
 * @ author       :  jlo
 * @ Date:  2018/12/16
 */
public class ScanTableForFilter {
    public static Configuration conf = HBaseConfiguration.create();

    public static void main(String[] args) {
        //scanTableByFilter("hbase_school","grade1","一年级","3");
        scanTableByFilterList("hbase_school","grade1","一年级","3","2");
    }

    private static void scanTableByFilterList(String tableName,String cf,String c,String... filterValue) {
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            //创建查询对象
            Scan scan = new Scan();
            //创建过滤条件
            List<Filter> list = new ArrayList<>();
            for(String s : filterValue){
                SingleColumnValueFilter filter = getSingleColumnValueFilter(cf,c,s);
                filter.setFilterIfMissing(true);
                list.add(filter);
            }
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, list);
            scan.setFilter(filterList);
            ResultScanner scanner = table.getScanner(scan);
            long count = 0;
            System.out.println("查询结果：");
            for(Result result : scanner){
                count++;
                printRow(result);
            }
            System.out.println(count + " rows()");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * @ Author jlo
     * @ Description 过滤还能有3的列值
     * @ Date 18:30 2018/12/16
     * @ Param [tableName, cf, c, filterValue]
     * @ return void
     **/
    private static void scanTableByFilter(String tableName,String cf,String c,String filterValue) {
        Connection conn = null;
        Table table = null;
        try {
            conn =ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            SingleColumnValueFilter filter = getSingleColumnValueFilter(cf, c, filterValue);
            scan.setFilter(filter);
            //获取的结果集
            ResultScanner scanner = table.getScanner(scan);
            long count =0;
            System.out.println("查询结果为: ");
            for(Result result : scanner){
                count++;
                printRow(result);
            }
            System.out.println(count + " rows()");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @ Author jlo
     * @ Description 创建过滤的对象
     * @ Date 18:37 2018/12/16
     * @ Param [cf, c, filterValue]
     * @ return org.apache.hadoop.hbase.filter.SingleColumnValueFilter
     **/
    private static SingleColumnValueFilter getSingleColumnValueFilter(String cf, String c, String filterValue) {
        //创建过滤的对象
        //包含过滤器
        SubstringComparator comparator = new SubstringComparator(filterValue);
        //正则过滤器
       // RegexStringComparator comparator2 = new RegexStringComparator("^10");
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf)
        ,Bytes.toBytes(c), CompareFilter.CompareOp.EQUAL,comparator);
        filter.setFilterIfMissing(true);
        return filter;
    }


    /**
     * @ Author jlo
     * @ Description 打印
     * @ Date 18:21 2018/12/16
     * @ Param [result]
     * @ return void
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
