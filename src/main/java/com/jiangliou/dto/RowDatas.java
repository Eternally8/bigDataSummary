package com.jiangliou.dto;

/**
 * @ ClassName    :  RowDatas
 * @ Description  :  Hbase插入的数据封装(可以是一条,也可以是多条)
 * @ author       :  jlo
 * @ Date:  2018/12/16
 */
public class RowDatas {

    /**
     * Hbase的rowKey主键
     * @ Date 14:22 2018/12/16
     **/
    private String rowKey;
    /**
     * Hbase列族
     * @ Date 14:22 2018/12/16
     **/
    private String cf;
    /**
     * Hbase 列
     * @ Date 14:22 2018/12/16
     **/
    private String c;

    /**
     * Hbase 时间戳
     * @ Date 14:22 2018/12/16
     **/
    private long ts = -1;

    /**
     * Hbase 插入的数据
     * @ Date 14:22 2018/12/16
     **/
    private String value;


    public RowDatas(String rowKey, String cf, String c, long ts, String value) {
        this.rowKey = rowKey;
        this.cf = cf;
        this.c = c;
        this.ts = ts;
        this.value = value;
    }

    public RowDatas (String rowKey, String cf, String c, String value) {
        this.rowKey = rowKey;
        this.cf = cf;
        this.c = c;
        this.value = value;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getCf() {
        return cf;
    }

    public void setCf(String cf) {
        this.cf = cf;
    }

    public String getC() {
        return c;
    }

    public void setC(String c) {
        this.c = c;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
