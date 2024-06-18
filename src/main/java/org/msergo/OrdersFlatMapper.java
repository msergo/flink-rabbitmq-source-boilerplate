package org.msergo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.msergo.models.CoreApiMessage;
import org.msergo.models.Order;

public class OrdersFlatMapper implements FlatMapFunction<CoreApiMessage, Order> {
    @Override
    public void flatMap(CoreApiMessage coreApiMessage, Collector<Order> out) {
        out.collect(coreApiMessage.getResult());
    }
}
