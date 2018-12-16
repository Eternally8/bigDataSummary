/**
 * OrcUtil.java
 * com.hainiuxy.mrrun.util
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.jiangliou.utils;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

/**
 * orc文件读写工具类<br/>
 * 读数据的流程：<br/>
 * 1）调用 setReadOrcInspector()，获取读数据的inspector对象<br/>
 * 2）调用 getOrcStructData()，获取指定字段的值<br/>
 * 
 * 写数据的流程：<br/>
 * 1）调用 setWriteOrcInspector()，获取写数据的inspector对象<br/>
 * 2）将要写入的数据按照orc schema 的顺序添加，掉addAttr()<br/>
 * 3）调用 serialize()，将要写入的数据序列化成指定orc格式的Writable<br/>
 * @author   潘牛                      
 * @Date	 2018年12月14日 	 
 */
public class OrcUtil {
	
	/**
	 * 读取orc文件的inspector对象
	 */
	StructObjectInspector inspectorR = null;
	
	/**
	 * 写orc文件的inspector对象
	 */
	ObjectInspector inspectorW = null;
	
	
	/**
	 * 用于存放一行数据
	 */
	List<Object> realRow = null;
	
	OrcSerde serde = null;

	/**
	 * 设置读取orc文件的inspector，如果你想读取orc文件，首先调用该方法
	 * @ param schema orc的schema
	*/
	public void setReadOrcInspector(String schema) {
		//根据schema 获取对应的typeinfo对象
		TypeInfo typeInfo= (TypeInfo)TypeInfoUtils.getTypeInfoFromTypeString(schema);
		//根据typeinfo对象，获取对应的inspector对象: OrcStructInspector
		inspectorR = (StructObjectInspector)OrcStruct.createObjectInspector(typeInfo);
	}
	
	/**
	 * 获取orcstruct对象中的指定字段的值
	 * 
	 * @param orcStruct 存数据的对象
	 * @param fieldName 字段名称
	 * @return 返回字段名称的value值
	*/
	public String getOrcStructData(OrcStruct orcStruct, String fieldName) {
		StructField structField = inspectorR.getStructFieldRef(fieldName);
		String value = String.valueOf(inspectorR.getStructFieldData(orcStruct, structField));
		
		return Utils.isEmpty(value) || value.equalsIgnoreCase("null") ? null : value;
	}

	/**
	 * 设置写入orc文件的inspector，如果你想写入orc文件，首先调用该方法
	 * @param schema orc的schema
	*/
	public void setWriteOrcInspector(String schema) {
		//根据schema 获取对应的typeinfo对象
		TypeInfo typeInfo= (TypeInfo)TypeInfoUtils.getTypeInfoFromTypeString(schema);
		//getStandardJavaObjectInspectorFromTypeInfo, javaobject
		inspectorW = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
		
	}

	/**
	 * 这是一个类似StringBuiler的append(), 可以连续添加，并且参数还不顾定
	 * @param objs 可变参数
	 * @return 返回OrcUtil对象
	*/
	public OrcUtil addAttr(Object... objs ) {
		if(realRow == null){
			realRow = new ArrayList<Object>();
		}
		for(Object obj : objs){
			realRow.add(obj);
		}
		return this;
	}

	/**
	 * 将一行的数据序列化成指定orc格式的Writable
	 * @return orc格式的Writable
	*/
	public Writable serialize() {
		if(this.serde == null){
			this.serde = new OrcSerde();
		}
		Writable w = this.serde.serialize(realRow, inspectorW);
		//序列化完成后，重新new list
		realRow = new ArrayList<Object>();
		return w;
		
	}
	
	

}

