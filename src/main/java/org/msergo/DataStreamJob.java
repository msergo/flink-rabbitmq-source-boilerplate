package org.msergo;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.msergo.models.CoreApiMessage;
import org.msergo.models.Order;
import org.msergo.sources.RabbitMQCustomSource;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
				.setHost("localhost")
				.setVirtualHost("/")
				.setUserName("user")
				.setPassword("bitnami")
				.setPort(5672)
				.build();


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStream<CoreApiMessage> stream = env
				.addSource(new RabbitMQCustomSource(connectionConfig, "core-api", "flink.orders"))
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.forGenerator(context -> new WatermarkGenerator<CoreApiMessage>() {
									private long latestTimestamp = 0;
									private final long maxOutOfOrderness = 5000; // 5 seconds

									@Override
									public void onEvent(CoreApiMessage coreApiMessage, long eventTimestamp, WatermarkOutput watermarkOutput) {
										this.latestTimestamp = eventTimestamp;
									}

									@Override
									public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
										long expectedTimestamp = this.latestTimestamp + this.maxOutOfOrderness;
										// If 5s already passed since the last event, advance the watermark and emit it
										if (latestTimestamp != 0 && expectedTimestamp < System.currentTimeMillis()) {
											System.err.println("Watermark: " + latestTimestamp);
											watermarkOutput.emitWatermark(new Watermark(expectedTimestamp));
											// Stream is idle, reset the watermark and wait for the next event
											latestTimestamp = 0;
										}
									}
								})
								.withTimestampAssigner((event, timestamp) -> event.getResult().getUpdatedAt().getTime())
				);

		// Example of a tumbling window with custom processing function
		DataStream<Order> ordersSessioned = stream
				.flatMap(new OrdersFlatMapper())
				.keyBy(orders -> orders.getId())
				.window(EventTimeSessionWindows.withGap(Time.seconds(5)))// Session window with 10-second interval
				.reduce((r1, r2) -> r2); // Keep the last element in the window

		ordersSessioned.printToErr();
        env.execute("Flink Java API Boilerplate");
    }
}
