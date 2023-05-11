package slidewindows.wtp.domain.converter;

import com.alibaba.fastjson.JSONObject;
import org.mapstruct.Mapper;
import slidewindows.wtp.debedium.DebeziumJsonRecordWithOutSchema;
import slidewindows.wtp.domain.CarSpeed;

@Mapper
public interface CarSpeedConverter {
    default CarSpeed recordToCarSpeed(Object record) {
        String s = JSONObject.toJSONString(record);
        CarSpeed carSpeed = JSONObject.parseObject(s, CarSpeed.class);
        return carSpeed;
    }
}
