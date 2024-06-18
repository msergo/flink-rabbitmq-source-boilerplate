package org.msergo.sources;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.msergo.models.CoreApiMessage;

import java.nio.charset.StandardCharsets;

public class CustomDeserializationSchema implements RMQDeserializationSchema<CoreApiMessage> {
    @Override
    public void deserialize(Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes, RMQCollector<CoreApiMessage> rmqCollector) {
        String message = new String(bytes, StandardCharsets.UTF_8);
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        CoreApiMessage coreApiMessage = gson.fromJson(message, CoreApiMessage.class);

        rmqCollector.collect(coreApiMessage);
    }

    @Override
    public boolean isEndOfStream(CoreApiMessage nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CoreApiMessage> getProducedType() {
        return TypeInformation.of(CoreApiMessage.class);
    }
}
