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
@RequestMapping("/v1")//映射路径
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
     * 接收客户端埋点上报的日志数据：用户活跃数据
     * @param jsonObj
     * @return
     */
    @RequestMapping(value="/uac",method = RequestMethod.POST,consumes="application/json")
    public Status userActive(@RequestBody(required=false)JSONObject jsonObj) {
        Status status = new Status();
        logger.info(jsonObj.toJSONString());
        return status;
    }




}
