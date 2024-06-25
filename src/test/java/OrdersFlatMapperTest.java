import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import org.msergo.OrdersFlatMapper;
import org.msergo.models.CoreApiMessage;
import org.msergo.models.Order;

import static org.mockito.Mockito.mock;

public class OrdersFlatMapperTest {
    @Test
    public void testOrdersFlatMapper() throws Exception {
        OrdersFlatMapper ordersFlatMapper = new OrdersFlatMapper();
        Collector<Order> collector = mock(Collector.class);

        CoreApiMessage coreApiMessage = new CoreApiMessage();
        Order order = new Order();
        order.setId("1");
        order.setCustomerId("2");
        order.setStatus("ready");
        order.setTotal(1L);

        coreApiMessage.setResult(order);

        ordersFlatMapper.flatMap(coreApiMessage, collector);
        Mockito.verify(collector, Mockito.times(1)).collect(order);
    }
}
