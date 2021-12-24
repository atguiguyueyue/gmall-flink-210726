package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDC_DataStream_Deserializer {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //TODO 2.设置FlinkCDC参数
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall210726")
                .username("root")
                .password("000000")
                .tableList("gmall210726.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute();


    }

    /**
     * {
     *     "databases":"gmall_210726",
     *     "tableName":"base_trademark",
     *     "type:"insert",
     *     "before":{"id":"101","name":"zs".....},
     *     "after":{"id":"101","name":"zs".....}
     * }
     */

    //自定义反序列化器
    public static class MyDeserial implements DebeziumDeserializationSchema<String>{

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //1.创建一个JSONObject用来存放结果数据
            JSONObject result = new JSONObject();

            //2.获取数据库名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");

            String database = split[1];

            //3.获取表名
            String tableName = split[2];

            //4.获取类型 insert update delete
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)){
                type = "insert";
            }

            //5.获取数据
            Struct value = (Struct) sourceRecord.value();

            //6.获取before数据

            JSONObject beforeJson = new JSONObject();
            Struct structBefore = value.getStruct("before");
            if (structBefore!=null){
                Schema schema = structBefore.schema();
                    List<Field> fields = schema.fields();
                    for (Field field : fields) {
                        beforeJson.put(field.name(), structBefore.get(field));
                    }
                }



            //7.获取after数据
            JSONObject afterJson = new JSONObject();
            Struct structAfter = value.getStruct("after");
            if (structAfter!=null){
                    Schema schema = structAfter.schema();
                    List<Field> fields = schema.fields();
                    for (Field field : fields) {
                        afterJson.put(field.name(), structAfter.get(field));
                    }
                }


            //将数据封装到JSONObject中
            result.put("database", database);
            result.put("tableName", tableName);
            result.put("before",beforeJson);
            result.put("after", afterJson);
            result.put("type", type);


            //将数据发送至下游
            collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
