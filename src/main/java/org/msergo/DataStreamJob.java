package org.msergo;

import com.typesafe.config.Config;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.msergo.config.ConfigManager;
import org.msergo.models.CoreApiMessage;
import org.msergo.models.Order;
import org.msergo.sources.RabbitMQCustomSource;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        Config config = ConfigManager.getConfig();

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(config.getString("rabbitmq.host"))
                .setVirtualHost("/")
                .setUserName(config.getString("rabbitmq.user"))
                .setPassword(config.getString("rabbitmq.password"))
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
                .keyBy(order -> order.getId())
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))// Session window with 10-second interval
                .reduce((r1, r2) -> r2); // Keep the last element in the window

        // Sink to PostgreSQL
        ordersSessioned
                .addSink(
                        JdbcSink.sink(
                                "insert into orders (id, customer_id, total, status, created_at, updated_at) values (?, ?, ?, ?, ?, ?)",
                                (statement, order) -> {
                                    statement.setString(1, order.getId());
                                    statement.setString(2, order.getCustomerId());
                                    statement.setLong(3, order.getTotal());
                                    statement.setString(4, order.getStatus());
                                    statement.setDate(5, new java.sql.Date(order.getCreatedAt().getTime()));
                                    statement.setDate(6, new java.sql.Date(order.getUpdatedAt().getTime()));
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(config.getString("db.connectionUrl"))
                                        .withDriverName(config.getString("db.driver"))
                                        .withUsername(config.getString("db.user"))
                                        .withPassword(config.getString("db.password"))
                                        .build()
                        ));


        env.execute("Flink Java API Boilerplate");
    }
}
