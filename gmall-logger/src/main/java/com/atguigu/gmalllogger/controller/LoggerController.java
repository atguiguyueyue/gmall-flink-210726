package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
@Slf4j
@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test(){
        System.out.println("1111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name, @RequestParam("age") Integer age){
        System.out.println("123");
        return "name:" + name + " age:" + age;
    }

    //用来接受moker包模拟的数据
    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){

//        System.out.println(jsonStr);
//        Logger logger = LoggerFactory.getLogger(LoggerController.class);
//        logger.info(jsonStr);
        //打印数据到控制台并落盘
        log.info(jsonStr);

        //将数据写入kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}
