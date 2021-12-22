package com.atguigu.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建表并连接MySQl
        tableEnv.executeSql("CREATE TABLE mysql_binlog (\n" +
                " tm_id STRING not null,\n" +
                " tm_name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'hadoop102',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '000000',\n" +
                " 'database-name' = 'gmall210726',\n" +
                " 'scan.incremental.snapshot.enabled' = 'false',\n" +
                " 'table-name' = 'base_trademark'\n" +
                ")");

        tableEnv.executeSql("select * from mysql_binlog").print();
    }
}
