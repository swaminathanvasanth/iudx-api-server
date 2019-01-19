package broker;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.serviceproxy.ServiceBinder;
import iudx.URLs;

public class BrokerVerticle extends AbstractVerticle
{
	@Override
	public void start(Future<Void> startFuture)
	{	
		RabbitMQOptions options = new RabbitMQOptions();
		options.setHost(URLs.getBrokerUrl());
		options.setPort(URLs.getBrokerPort());
		options.setVirtualHost(URLs.getBrokerVhost());
		options.setUser(URLs.getBrokerUsername());
		options.setPassword(URLs.getBrokerPassword());
		options.setConnectionTimeout(6000);
		options.setRequestedHeartbeat(60);
		options.setHandshakeTimeout(6000);
		options.setRequestedChannelMax(5);
		options.setNetworkRecoveryInterval(500);
		
		BrokerService brokerService		= new BrokerServiceImpl(vertx, options);
		ServiceBinder binder			= new ServiceBinder(vertx);
		
		binder	
		.setAddress("broker.queue")
		.register(BrokerService.class, brokerService);
		
		startFuture.complete();
	}
	
}
