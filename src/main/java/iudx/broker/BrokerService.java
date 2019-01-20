package iudx.broker;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.rabbitmq.RabbitMQOptions;

@ProxyGen
@VertxGen
public interface BrokerService 
{
	@Fluent
	BrokerService create_owner_resources(String id, Handler<AsyncResult<Void>> resultHandler);
	
	@Fluent
	BrokerService delete_owner_resources(String id, Handler<AsyncResult<Void>> resultHandler);
	
	@Fluent
	BrokerService create_owner_bindings(String id, Handler<AsyncResult<Void>> resultHandler);
	
	@Fluent
	BrokerService create_entity_resources(String id, Handler<AsyncResult<Void>> resultHandler);
	
	@Fluent
	BrokerService delete_entity_resources(String id_list, Handler<AsyncResult<Void>> resultHandler);
	
	@Fluent
	BrokerService create_entity_bindings(String id, Handler<AsyncResult<Void>> resultHandler);
	
	@GenIgnore
	static BrokerService create(Vertx vertx, RabbitMQOptions options, Handler<AsyncResult<BrokerService>> resultHandler)
	{
		return new BrokerServiceImpl(vertx, options, resultHandler);
	}
	
	@GenIgnore
	static BrokerService createProxy(Vertx vertx, String address)
	{
		return new BrokerServiceVertxEBProxy(vertx, address);
	}
	
}
