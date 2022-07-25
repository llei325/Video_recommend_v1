package com.imooc.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.imooc.data.utils.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * 【服务端日志】模拟生成实时粉丝关注和取消关注数据
 * 然后调用服务端接口，记录日志数据
 *
 * Created by xuwei
 */
public class GenerateRealTimeFollowData {
    private final static Logger logger = LoggerFactory.getLogger(GenerateRealTimeFollowData.class);
    public static void main(String[] args) throws Exception{
        //通过接口获取实时粉丝关注和取消关注数据
        String dataUrl = "http://data.xuwei.tech/vr1/ur";
        JSONObject paramObj = new JSONObject();
        //TODO code:校验码，需要到微信公众号上获取有效校验码,具体操作流程见电子书
        paramObj.put("code","imooc");//校验码
        JSONObject dataObj = HttpUtil.doPost(dataUrl, paramObj);
        //logger.info(dataObj.toJSONString());
        //判断数据是否获取成功
        boolean flag = dataObj.containsKey("error");
        if(!flag){
            String data = dataObj.getString("data");
            //调用服务端接口，记录日志数据
            dataUrl = "http://bigdata04:8081/s1/ur";
            JSONObject resObj = HttpUtil.doPost(dataUrl, data);
            String msg = resObj.getString("msg");
            if("success".equals(msg)){
                logger.info("接口调用成功："+data);
            }else{
                logger.error("接口调用失败");
            }
        }else{
            logger.error("获取粉丝数据失败："+dataObj.toJSONString());
        }

    }
}
