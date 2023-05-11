package slidewindows.wtp;

import org.apache.flink.api.common.functions.ReduceFunction;

public class CustomerReduceFunction implements ReduceFunction<Integer> {
    @Override
    public Integer reduce(Integer value1, Integer value2) throws Exception {
        return value1 + value2;
    }
}
