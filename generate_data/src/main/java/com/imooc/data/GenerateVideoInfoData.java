package com.imooc.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.imooc.data.utils.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;

/**
 * 【服务端日志】模拟生成视频相关数据
 * 然后调用服务端接口，记录日志数据
 * Created by xuwei
 */
public class GenerateVideoInfoData {
    private final static Logger logger = LoggerFactory.getLogger(GenerateRealTimeFollowData.class);
    public static void main(String[] args) throws Exception{
        //通过接口获取视频相关数据信息
        String dataUrl = "http://data.xuwei.tech/vr1/vi";
        JSONObject paramObj = new JSONObject();
        //TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code","imooc");//校验码
        paramObj.put("date","2026-02-01");//日期
        JSONObject dataObj = HttpUtil.doPost(dataUrl, paramObj);
        //logger.info(dataObj.toJSONString());
        //判断数据是否获取成功
        boolean flag = dataObj.containsKey("error");
        if(!flag){
            JSONArray jsonArray = dataObj.getJSONArray("data");
            for(int i=0;i<jsonArray.size();i++){
                JSONObject jsonObj = jsonArray.getJSONObject(i);
                String line = jsonObj.getString("line");
                //调用服务端接口，记录日志数据
                dataUrl = "http://bigdata04:8081/s1/vi";
                JSONObject resObj = HttpUtil.doPost(dataUrl, line);
                String msg = resObj.getString("msg");
                if("success".equals(msg)){
                    logger.info("接口调用成功");
                }else{
                    logger.error("接口调用失败");
                }
            }

        }else{
            logger.error("获取视频数据失败："+dataObj.toJSONString());
        }

    }
}
