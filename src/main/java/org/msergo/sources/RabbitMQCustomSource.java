package org.msergo.sources;

import com.rabbitmq.client.AMQP;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.msergo.models.CoreApiMessage;

import java.io.IOException;

public class RabbitMQCustomSource extends RMQSource<CoreApiMessage> {
    private final String exchangeName;

    public RabbitMQCustomSource(RMQConnectionConfig rmqConnectionConfig, String exchangeName, String queueName) {
        super(rmqConnectionConfig, queueName, new CustomDeserializationSchema());
        this.exchangeName = exchangeName;
    }

    @Override
    protected void setupQueue() throws IOException {
        channel.exchangeDeclare(this.exchangeName, "topic", true);
        AMQP.Queue.DeclareOk queueDeclareOk = channel.queueDeclare(this.queueName, true, false, false, null);
        channel.queueBind(queueDeclareOk.getQueue(), this.exchangeName, "orders.#");
    }
}
