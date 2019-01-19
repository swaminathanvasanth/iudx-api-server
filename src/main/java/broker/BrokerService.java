package broker;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

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
	
}
