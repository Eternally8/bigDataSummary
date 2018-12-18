package com.jiangliou.hbase.scanHbase2Orc;

import com.jiangliou.base.BaseMR;
import com.jiangliou.utils.Constants;
import com.jiangliou.utils.OrcFormat;
import com.jiangliou.utils.OrcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ ClassName    :  ScanHbase2Orc
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/18
 */
public class ScanHbase2Orc extends BaseMR {
    /**
     * KEYIN, VALUEIN,
     * 通过TableMapReduceUtil.initTableMapperJob(), 确定inputformat是 TableInputFormat
     * 这个TableInputFormat extends TableInputFormatBase，
     * 而 public abstract class TableInputFormatBase extends InputFormat<ImmutableBytesWritable, Result>
     *
     * keyin:ImmutableBytesWritable
     * valuein:Result
     * -----------------------------------
     * KEYOUT, VALUEOUT
     * 通过OrcNewOutputFormat: orc输出的格式
     * public class OrcNewOutputFormat extends FileOutputFormat<NullWritable, OrcSerdeRow>
     * 由因为 OrcSerdeRow 是个内部类，不能引入OrcSerdeRow类，只能引入Writable
     * 能引入Writable的原因是， OrcSerdeRow implements Writable
     * KEYOUT:NullWritable
     * VALUEOUT:Writable
     * -----------------------------------
     * 因为TableMapReduceUtil.initTableMapperJob()输入参数有个mapperclass, 这个mapper必须继承TableMapper,
     * 查看TableMapper：
     * public abstract class TableMapper<KEYOUT, VALUEOUT> extends Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
     * keyin,valuein 已经固定，并且是我们想要的类型
     * 所以可以直接继承Mapper
     *
     *  public static class ScanHbase2OrcMapper extends Mapper<ImmutableBytesWritable, Result, NullWritable, Writable>{
     */
    public static class ScanHbase2OrcMapper extends TableMapper<NullWritable, Writable> {
        static SimpleDateFormat dfs = new SimpleDateFormat("yyyyMMddHHmmss");
        OrcUtil orcUtil = new OrcUtil();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            orcUtil.setWriteOrcInspector(OrcFormat.orcSchema);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //key:rowKey
            //result:一个rowKey的一行数据
            //aid_yyyyMMddHHmmss
            String rowkey = Bytes.toString(key.get());
            String aid = rowkey.split("_")[0];
            String uptimestr = rowkey.split("_")[1];
            //将时间戳转换为日期
            Date date = null;
            try {
                date = dfs.parse(uptimestr);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            //转换为秒数
            long uptime = date.getTime()/1000;
            String pkgname = null, typestr =null, country = null, gpcategory = null;
            int type = -1;
            Cell[] rawCells = value.rawCells();
            for(Cell cell :rawCells){
                String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String colValue = Bytes.toString(CellUtil.cloneValue(cell));
                switch (colName){
                    case "pkgname"  : pkgname = colValue;break;
                    case "type"			: typestr = colValue; break;
                    case "country"		: country = colValue; break;
                    case "gpcategory"	: gpcategory = colValue; break;
                    default: break;
                }
            }
            if(null == typestr){
                type = -1;
            }else {
                type = Integer.parseInt(typestr);
            }
            System.out.println("----------------------------------");
            System.out.println("aid         :" + aid        );
            System.out.println("pkgname     :" + pkgname    );
            System.out.println("uptime      :" + uptime  );
            System.out.println("type        :" + type       );
            System.out.println("country     :" + country    );
            System.out.println("gpcategory  :" + gpcategory );
//            -----------写orc------------------------------------------------

//			aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string
//			realRow.add 顺序是一致的
            orcUtil.addAttr(aid, pkgname, uptime, type, country, gpcategory);
            Writable w = orcUtil.serialize();
            context.write(NullWritable.get(),w);
        }
    }



    @Override
    public Job getJob(Configuration conf) throws IOException {
        //关闭map的推测执行，使得一个map处理 一个region的数据
        conf.set("mapreduce.map.spedulative","false");
        //设置orc文件 有索引
        conf.set("orc.create.index", "true");

        Job job = Job.getInstance(conf, getJobNameWithTaskId());
        job.setJarByClass(ScanHbase2Orc.class);
        //没有reduce
        job.setNumReduceTasks(0);
        //输出orc文件的format
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        TableName tableName = TableName.valueOf(conf.get(Constants.HBASE_TABLE_NAME_ATTR));
        Scan scan = new Scan();
        //查询结果,作为inputformat核心代码设置
        TableMapReduceUtil.initTableMapperJob(tableName,scan,ScanHbase2OrcMapper.class,NullWritable.class,Writable.class,job);
        //无输入目录，只有输出
        FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskId()));
        return job;
    }

    @Override
    public String getJobName() {
        return "scanhbase2orc";
    }
}
