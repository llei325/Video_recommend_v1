package com.imooc.controller;

import com.alibaba.fastjson.JSONObject;
import com.imooc.bean.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

/**
 * 数据接口V1.0
 * Created by xuwei
 */
@RestController//控制器类
@RequestMapping("/s1")//映射路径
public class DataController {
    private static final Logger logger = LoggerFactory.getLogger(DataController.class);
    /**
     * 测试接口
     * @param name
     * @return
     */
    @RequestMapping(value="/t1",method = RequestMethod.GET)
    public Status test(@RequestParam("name") String name) {
        Status status = new Status();
        System.out.println(name);
        return status;
    }

    /**
     * 粉丝关注功能：会记录下粉丝关注数据和取消关注数据
     * 服务端接口，被直播平台调用的时候记录数据
     * @param jsonObj
     * @return
     */
    @RequestMapping(value="/ur",method = RequestMethod.POST,consumes="application/json")
    public Status userRelation(@RequestBody(required=false)JSONObject jsonObj) {
        Status status = new Status();
        logger.info(jsonObj.toJSONString());
        return status;
    }

    /**
     * 直播结束的时候会调用这个接口：记录视频(直播)相关数据
     * 服务端接口，被直播平台调用的时候记录数据
     * @param jsonObj
     * @return
     */
    @RequestMapping(value="/vi",method = RequestMethod.POST,consumes="application/json")
    public Status videoInfo(@RequestBody(required=false)JSONObject jsonObj) {
        Status status = new Status();
        logger.info(jsonObj.toJSONString());
        return status;
    }



}
