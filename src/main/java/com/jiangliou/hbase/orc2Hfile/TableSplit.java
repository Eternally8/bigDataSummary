package com.jiangliou.hbase.orc2Hfile;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @ ClassName    :  TableSplit
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/16
 */
public class TableSplit implements SplitAlgorithm {
    @Override
    public byte[] split(byte[] start, byte[] end) {
        return new byte[0];
    }

    @Override
    public byte[][] split(int numRegions) {
        //读取分隔region的配置文件,读取每一行,放到byte[]中
        List<byte[]> regions = new ArrayList<>();
        Set<String> set = readSplitData();
        for(String str : set){
            regions.add(Bytes.toBytes(str));
        }
        //将list转换成数组，如果list带有泛型，转换成指定泛型的数组，需要用toArray(T[])
        //如果只想转换成Object[] ,可以用toArray()
        return new byte[0][];
    }

    /**
     * @ Author jlo
     * @ Description 读取分隔region的配置文件, 读取每一行放到set集合
     * @ Date 21:09 2018/12/16
     * @ Param []
     * @ return java.util.Set<java.lang.String>
     **/
    private Set<String> readSplitData() {
        Set<String> set = new HashSet<>();
        try {
            InputStream is = TableSplit.class.getResourceAsStream("/split_data.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
            String line = null;
            while((line = reader.readLine()) != null){
                set.add(line);
            }
            System.out.println("set.size():" + set.size());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return set;
    }

    @Override
    public byte[] firstRow() {
        return new byte[0];
    }

    @Override
    public byte[] lastRow() {
        return new byte[0];
    }

    @Override
    public void setFirstRow(String userInput) {

    }

    @Override
    public void setLastRow(String userInput) {

    }

    @Override
    public byte[] strToRow(String input) {
        return new byte[0];
    }

    @Override
    public String rowToStr(byte[] row) {
        return null;
    }

    @Override
    public String separator() {
        return null;
    }

    @Override
    public void setFirstRow(byte[] userInput) {

    }

    @Override
    public void setLastRow(byte[] userInput) {

    }
}
