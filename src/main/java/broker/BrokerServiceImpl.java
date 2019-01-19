package broker;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

public class BrokerServiceImpl implements BrokerService
{
	Map<String, RabbitMQClient> adminpool;
	RabbitMQClient client;
	RabbitMQOptions options;
	int count;
	Vertx vertx;
	
	BrokerServiceImpl(Vertx vertx, RabbitMQOptions options/**, Handler<AsyncResult<Void>> result**/)
	{
		this.options	= 	options;
		this.vertx		= 	vertx;
		adminpool		= 	new HashMap<String, RabbitMQClient>();
		
		getAdminChannel();
	
	}
	
	public Future<Void> getAdminChannel()
	{
		Future<Void> init	=	Future.future();
		
		if(!adminpool.containsKey("admin"))
		{
			client = RabbitMQClient.create(vertx, options);
			
			client.start(ar -> {
				
				if(ar.succeeded())
				{
					init.complete();
					adminpool.put("admin", client);
				}
			});
		}
		else if(!adminpool.get("admin").isOpenChannel())
		{
			client = RabbitMQClient.create(vertx, options);
			
			client.start(ar -> {
				
				if(ar.succeeded())
				{
					init.complete();
					adminpool.put("admin", client);
				}
			});
		}
		else
		{
			init.complete();
		}
		
		return init;
	}

	@Override
	public BrokerService create_owner_resources(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		count	=	2;
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel.exchangeDeclare(id+".notification", "topic", true, false, result -> 
				{
					if(result.succeeded())
					{
						count--;
						if(count==0)resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
						
				adminChannel.queueDeclare(id+".notification", true, false, false, 
				result -> 
				{
					if(result.succeeded())
					{
						count--;
						if(count==0)resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
		});
		
		
		
		return this;
	}

	@Override
	public BrokerService delete_owner_resources(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		count 		= 2;
		
		System.out.println("owner id = " + id);
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				RabbitMQClient adminChannel = adminpool.get("admin");
		
				adminChannel.exchangeDelete(id+".notification", result -> {
			
					if(result.succeeded())
					{
						count--;
						if(count == 0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
		
				adminChannel.queueDelete(id+".notification", result -> {
			
					if(result.succeeded())
					{
						count--;
						System.out.println("deleted notification queue");
						if(count == 0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
		});
		
		return this;
	}

	@Override
	public BrokerService create_owner_bindings(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		count		= 1;
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel.queueBind(id+".notification", id+".notification", "#",	result ->
				{
					if(result.succeeded())
					{
						count--;
						if(count==0)resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
		});
		
		return this;
	}

	@Override
	public BrokerService create_entity_resources(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		count 		= 11;
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				//create exchanges
				adminChannel.exchangeDeclare(id+".public", "topic", true, false, result -> {
			
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
		
				adminChannel.exchangeDeclare(id+".protected", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".private", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".notification", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".publish", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".diagnostics", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
						
				//create queues
				adminChannel.queueDeclare(id, true, false, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".private", true, false, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".priority", true, false, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".command", true, false, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".notification", true, false, false, result -> {
					
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
		});
		
		return this;
	}

	@Override
	public BrokerService delete_entity_resources(String id_list, Handler<AsyncResult<Void>> resultHandler) 
	{
		System.out.println("in delete entity resources");
		String ids[]	= id_list.split(",");
		count			= (11 * ids.length);
		
		Future<Void>	channel	=	getAdminChannel();
	
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				// TODO: Do not use a plain loop. Use vertx.executeBlocking
				for(String id:ids)
				{
					if(id == "") continue;
					adminChannel.exchangeDelete(id+".public", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".protected", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".private", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".notification", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".publish", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".diagnostics", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
						
					//create queues
					adminChannel.queueDelete(id, result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".private", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".priority", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".command", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".notification", result -> {
						
						if(result.succeeded())
						{
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
				}
			}
		});
		return this;
	}
	
	@Override
	public BrokerService create_entity_bindings(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		count 		= 2;
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel
				.queueBind(id+".notification", id+".notification", "#", 
				result -> 
				{
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel
				.queueBind(id+".private", id+".private", "#", 
				result -> 
				{
					if(result.succeeded())
					{
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
		
			}
		});
		return this;
	}
}
