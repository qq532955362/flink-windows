package slidewindows.wtp;

import lombok.Data;
import org.apache.flink.api.common.eventtime.RecordTimestampAssigner;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import slidewindows.wtp.domain.CarSpeed;

import java.time.Duration;

public class EntityEventWatermarkStrategy implements WatermarkStrategy<CarSpeed> {


    @Override
    public TimestampAssigner<CarSpeed> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (SerializableTimestampAssigner<CarSpeed>) (element, recordTimestamp) -> {
            return element.getCreateTime(); // 告诉程序数据源里的时间戳是哪一个字段
        };
    }

    @Override
    public WatermarkGenerator<CarSpeed>
    createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomPeriodicGenerator();
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<CarSpeed> {
        private Long delayTime = 5000L; // 延迟时间 允许延迟5000ms
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(CarSpeed event, long eventTimestamp, WatermarkOutput
                output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.getCreateTime(), maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认 200ms 调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }

    }

    @Override
    public WatermarkStrategy<CarSpeed> withIdleness(Duration idleTimeout) {
        return WatermarkStrategy.super.withIdleness(Duration.ofSeconds(10));
    }
}
