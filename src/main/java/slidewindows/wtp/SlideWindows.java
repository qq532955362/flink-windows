package slidewindows.wtp;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.mapstruct.factory.Mappers;
import slidewindows.wtp.debedium.DebeziumJsonRecordWithOutSchema;
import slidewindows.wtp.debedium.DebeziumOptionTypeEnum;
import slidewindows.wtp.domain.converter.CarSpeedConverter;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SlideWindows {

    private static CarSpeedConverter converter = Mappers.getMapper(CarSpeedConverter.class);

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //debezium deserialization
        JsonDebeziumDeserializationSchema jsonDebeziumDeserializationSchema = new JsonDebeziumDeserializationSchema(false);
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        //source
        MySqlSource<String> mysqlCdcSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("flink-cdc")
                .password("flink-cdc@2023")
                .databaseList("wtp")
                .tableList("wtp.car_speed")
                .deserializer(jsonDebeziumDeserializationSchema).debeziumProperties(properties)
                .build();


        /*
        sEnv.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "sliding window count").filter(str -> {
            DebeziumJsonRecordWithOutSchema record = JSONObject.parseObject(str, DebeziumJsonRecordWithOutSchema.class);
            return DebeziumOptionTypeEnum.C.getOp().equals(record.getOp());
        }).map(str -> {
                    DebeziumJsonRecordWithOutSchema record = JSONObject.parseObject(str, DebeziumJsonRecordWithOutSchema.class);
                    CarSpeed carSpeed = converter.recordToCarSpeed(record.getAfter());
                    return carSpeed;
                }
        ).assignTimestampsAndWatermarks(new EntityEventWatermarkStrategy())
                .setParallelism(1).windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10))).sum("carId").print().setParallelism(1);

         */

        AtomicInteger t = new AtomicInteger(1);


        sEnv.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "sliding window count").filter(str -> {
            DebeziumJsonRecordWithOutSchema record = JSONObject.parseObject(str, DebeziumJsonRecordWithOutSchema.class);
            return DebeziumOptionTypeEnum.C.getOp().equals(record.getOp());
        }).map(str -> {
                    DebeziumJsonRecordWithOutSchema record = JSONObject.parseObject(str, DebeziumJsonRecordWithOutSchema.class);
                    return converter.recordToCarSpeed(record.getAfter());
                }
        ).assignTimestampsAndWatermarks(new EntityEventWatermarkStrategy())
                .setParallelism(1)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((pre, suf) -> {
                    if (pre.getSpeed().compareTo(suf.getSpeed()) >= 0) {
                        return pre;
                    }
                    return suf;
                })
                .map(a -> String.format("SystemCurrent:%s 当前计数%d,一分钟内最大速度的车是: %s ,速度为: %s ,创建时间为：%s", new Date(System.currentTimeMillis()), t.getAndIncrement(), a.getCarId(), a.getSpeed(), new Date(a.getCreateTime())))
                .print();

        sEnv.execute("MysqlCdc Window example11111");
    }

}
