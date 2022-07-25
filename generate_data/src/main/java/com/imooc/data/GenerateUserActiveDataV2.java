package com.imooc.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.imooc.data.utils.HdfsOpUtil;
import com.imooc.data.utils.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 【客户端日志】模拟生成用户活跃数据
 * 直接将数据上传到HDFS目录里面
 * Created by xuwei
 */
public class GenerateUserActiveDataV2 {
    private final static Logger logger = LoggerFactory.getLogger(GenerateUserActiveDataV2.class);
    public static void main(String[] args) throws Exception{
        //通过接口获取历史粉丝关注数据
        String dataUrl = "http://data.xuwei.tech/vr1/ua";
        JSONObject paramObj = new JSONObject();
        //TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code","imooc");//校验码
        String dt = "2026-02-01";
        paramObj.put("date", dt);//日期
        JSONObject dataObj = HttpUtil.doPost(dataUrl, paramObj);
        //logger.info(dataObj.toJSONString());
        //判断数据是否获取成功
        boolean flag = dataObj.containsKey("error");
        if(!flag){
            JSONArray jsonArray = dataObj.getJSONArray("data");
            StringBuffer sb = new StringBuffer();
            for(int i=0;i<jsonArray.size();i++){
                JSONObject jsonObj = jsonArray.getJSONObject(i);
                String line = jsonObj.getString("line");
                if(i==0){
                    sb.append(line);
                }else{
                    sb.append("\n"+line);
                }
            }
            //将数据上传到HDFS上面，注意：需要关闭HDFS的权限校验机制
            String hdfsOutPath = "hdfs://bigdata01:9000/data/user_active/"+dt.replace("-","");
            String fileName = "user_active"+"-"+dt+".log";
            logger.info("开始上传："+hdfsOutPath+"/"+fileName);
            HdfsOpUtil.put(sb.toString(),hdfsOutPath,fileName);
        }else{
            logger.error("获取主播等级数据失败："+dataObj.toJSONString());
        }

    }
}
