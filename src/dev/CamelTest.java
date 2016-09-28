package dev;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.apache.log4j.BasicConfigurator;
import org.apache.camel.component.netty.ChannelHandlerFactories;
import org.apache.camel.component.netty.ChannelHandlerFactory;

public class CamelTest {

	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
		
		ChannelHandlerFactory decoder = ChannelHandlerFactories.newLengthFieldBasedFrameDecoder(2048, 0, 4, 0, 4);

		SimpleRegistry reg = new SimpleRegistry();
		reg.put("decoder", decoder);
		CamelContext context = new DefaultCamelContext(reg);
		context.addRoutes(new RouteBuilder() {

			public void configure() {
				from("kafka:127.0.0.1:9092?topic=dhbw&groupId=testing&autoOffsetReset=earliest&consumersCount=1")
						.process(new Processor() {
							@Override
							public void process(Exchange exchange) throws Exception {
								String messageKey = "";
								if (exchange.getIn() != null) {
									Message message = exchange.getIn();
									Integer partitionId = (Integer) message.getHeader(KafkaConstants.PARTITION);
									String topicName = (String) message.getHeader(KafkaConstants.TOPIC);
									if (message.getHeader(KafkaConstants.KEY) != null)
										messageKey = (String) message.getHeader(KafkaConstants.KEY);
									Object data = message.getBody();

									System.out.println("topicName :: " + topicName + " partitionId :: " + partitionId
											+ " messageKey :: " + messageKey + " message :: " + data + "\n");
								}
							}
						}).to("log:input");
			}

		});

		context.start();
		System.out.println("Press ENTER to exit");
		System.in.read();

		System.out.println("exit");
		context.stop();
	}

}
