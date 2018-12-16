/**
 * BaseMR.java
 * com.hainiuxy.mrrun.base
 * Copyright (c) 2018, 海牛版权所有.
 * @ author   潘牛
*/

package com.jiangliou.base;

import com.jiangliou.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import java.io.IOException;

/**
 * mapreduce公共基类
 * @ author   蒋利欧
 * @ Date	 2018年11月26日
 */
public abstract class BaseMR {
	
	private static Configuration conf;
	
	public void setConf(Configuration conf){
		BaseMR.conf = conf;
	}
	
	public ControlledJob getControlledJob()throws IOException{
		//删除输出目录
		Path outputPath = getJobOutputPath(getJobNameWithTaskId());
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
			System.out.println("delete outputdir: " + outputPath.toString());
		}
		//创建ControlledJob对象
		ControlledJob cJob = new ControlledJob(conf);
		//获取job对象，由子类去实现
		Job job = getJob(conf);
		//设置job和ControlledJob对象的关系
		cJob.setJob(job);
		return cJob;
	}

	/**
	 * 获取job对象，由子类去按照具体的执行任务获取个性化job对象
	*/
	public abstract Job getJob(Configuration conf)throws IOException;
	
	/**
	 * 单纯的任务名称，由子类去个性化实现
	*/
	public abstract String getJobName();
	
	
	/**
	 * 得到的个性化的任务名称
	 * wordcount_1127
	 * @ return 个性化的任务名称
	*/
	public String getJobNameWithTaskId(){
		return getJobName() + "_" + conf.get(Constants.TASK_ID_ATTR);
	}
	
	/**
	 * 获取第一个任务的输入目录，通过获取-D参数传过来的输入目录
	 * wordcount
	 * input: /tmp/mr/task/input    首个任务输入目录， -Dtask.input.dir=/tmp/mr/task/input
	 * @ return Path对象
	*/
	public Path getFirstJobInputPath(){
		return new Path(conf.get(Constants.TASK_INPUT_DIR_ATTR));
	}
	
	
	/**
	 * 获取某个任务的输出目录
	 * output:/tmp/mr/task/wordcount_1126_panniu   输出目录：basePath+ / + 个性化的任务名称<br/>
	 * basePath=/tmp/mr/task  basePath 也通过-D参数传，  -Dtask.base.path=/tmp/mr/task<br/>
	 * @ return Path对象
	*/
	public Path getJobOutputPath(String jobName){
		return new Path(conf.get(Constants.TASK_BASE_DIR_ATTR) + "/" + jobName);
	}

}

