package com.imooc.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.imooc.data.utils.HttpUtil;
import com.imooc.data.utils.MyDbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * 【服务端数据库】模拟生成主播等级数据
 * Created by xuwei
 */
public class GenerateUserLevelData {
    private final static Logger logger = LoggerFactory.getLogger(GenerateUserLevelData.class);
    public static void main(String[] args) throws Exception{
        //通过接口获取历史粉丝关注数据
        String dataUrl = "http://data.xuwei.tech/vr1/ul";
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
            long start = System.currentTimeMillis();
            logger.info("===============start init===============");
            ArrayList<String> tmpSqlList = new ArrayList<String>();
            for(int i=0;i<jsonArray.size();i++){
                JSONObject jsonObj = jsonArray.getJSONObject(i);
                String line = jsonObj.getString("line");
                String sql = "insert into cl_level_user values ("+line+");";
                tmpSqlList.add(sql);
                if(tmpSqlList.size()%100==0){
                    MyDbUtils.batchUpdate(tmpSqlList);
                    tmpSqlList.clear();
                }
            }
            //把剩余的数据批量添加到数据库中
            MyDbUtils.batchUpdate(tmpSqlList);
            logger.info("===============end init==============");
            long end = System.currentTimeMillis();
            logger.info("===============耗时: "+(end-start)/1000+"秒===============");
        }else{
            logger.error("获取主播等级数据失败："+dataObj.toJSONString());
        }

    }
}
