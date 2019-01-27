package iudx.broker;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.rabbitmq.RabbitMQOptions;
import iudx.database.DbServiceImpl;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rabbitmq.RabbitMQClient;

public class BrokerServiceImpl implements BrokerService
{
	Map<String, RabbitMQClient> adminpool;
	RabbitMQClient client;
	RabbitMQOptions options;
	int count;
	Vertx vertx;
	
	private final static Logger logger = LoggerFactory.getLogger(DbServiceImpl.class);
	
	BrokerServiceImpl(Vertx vertx, RabbitMQOptions options, Handler<AsyncResult<BrokerService>> result)
	{
		this.options	= 	options;
		this.vertx		= 	vertx;
		adminpool		= 	new HashMap<String, RabbitMQClient>();
		client			=	RabbitMQClient.create(vertx, options);
		
		client.start(resultHandler -> {
			
			if(resultHandler.succeeded())
			{
				logger.debug("Rabbitmq client started");
				adminpool.put("admin", client);
				result.handle(Future.succeededFuture(this));
			}
			else
			{
				logger.debug(resultHandler.cause());
				result.handle(Future.failedFuture(resultHandler.cause()));
			}
		});
	
	}
	
	public Future<Void> getAdminChannel()
	{
		Future<Void> init	=	Future.future();
		
		if(!adminpool.containsKey("admin"))
		{
			logger.debug("Pool does not contain key");
			client = RabbitMQClient.create(vertx, options);
			
			client.start(ar -> {
				
				if(ar.succeeded())
				{
					adminpool.put("admin", client);
					init.complete();
					
				}
				else
				{
					System.out.println(ar.cause());
				}
			});
		}
		else if(!adminpool.get("admin").isOpenChannel())
		{
			logger.debug("Pool contains key. But channel is closed");
			client = RabbitMQClient.create(vertx, options);
			
			client.start(ar -> {
				
				if(ar.succeeded())
				{
					adminpool.put("admin", client);
					init.complete();
				}
				else
				{
					System.out.println(ar.cause());
				}
			});
		}
		else
		{
			logger.debug("Pool contains key and channel is open");
			init.complete();
		}
		
		return init;
	}

	@Override
	public BrokerService create_owner_resources(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		logger.debug("in create owner resources");
		
		count	=	2;
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel.exchangeDeclare(id+".notification", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created notification exchange");
						count--;
						if(count == 0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Failed to create notification exchange. Cause = "+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				
				adminChannel.queueDeclare(id+".notification", true, false, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created notification queue");
						count--;
						if(count == 0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Failed to create notification exchange. Cause = "+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
		
		
		
		return this;
	}

	@Override
	public BrokerService delete_owner_resources(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		logger.debug("In delete owner resources");
		
		count 		= 2;
		
		System.out.println("owner id = " + id);
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
		
				adminChannel.exchangeDelete(id+".notification", result -> {
			
					if(result.succeeded())
					{
						logger.debug("Deleted notification exchange");
						
						count--;
						if(count == 0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not delete notification exchange. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
		
				adminChannel.queueDelete(id+".notification", result -> {
			
					if(result.succeeded())
					{
						logger.debug("Deleted notification queue");
						count--;
						if(count == 0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not delete notification queue. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
		
		return this;
	}

	@Override
	public BrokerService create_owner_bindings(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		logger.debug("In create owner bindings");
		
		count		= 1;
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel.queueBind(id+".notification", id+".notification", "#",	result ->
				{
					if(result.succeeded())
					{
						logger.debug("Bound notification queue to notification exchange");
						count--;
						if(count==0)resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Bind failed. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
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
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				//create exchanges
				adminChannel.exchangeDeclare(id+".public", "topic", true, false, result -> {
			
					if(result.succeeded())
					{
						logger.debug("Created public exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create public exchange. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
		
				adminChannel.exchangeDeclare(id+".protected", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created protected exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create protected exchange. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".private", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created private exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create private exchange. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".notification", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created notification exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create notification exchange. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".publish", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created publish exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create publish exchange. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.exchangeDeclare(id+".diagnostics", "topic", true, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created diagnostics exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create diagnostics exchange. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
						
				//create queues
				adminChannel.queueDeclare(id, true, false, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created regular queue");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create regular queue. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".private", true, false, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created private queue");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create private queue. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".priority", true, false, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created priority queue");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create priority queue. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".command", true, false, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created command queue");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create command queue. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel.queueDeclare(id+".notification", true, false, false, result -> {
					
					if(result.succeeded())
					{
						logger.debug("Created notification queue");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Could not create notification queue. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
		
		return this;
	}

	@Override
	public BrokerService delete_entity_resources(String id_list, Handler<AsyncResult<Void>> resultHandler) 
	{
		logger.debug("In delete entity resources");
		
		String ids[]	= id_list.split(",");
		count			= (11 * ids.length);
		
		logger.debug("id_list="+Arrays.asList(ids));
		
		if(ids[0].equals(""))
		{
			resultHandler.handle(Future.succeededFuture());
			return this;
		}
		
		Future<Void>	channel	=	getAdminChannel();
	
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				// TODO: Do not use a plain loop. Use vertx.executeBlocking
				for(String id:ids)
				{
					if(id == "") continue;
					
					//delete exchanges
					adminChannel.exchangeDelete(id+".public", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted public exchange");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete public exchange. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".protected", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted protected exchange");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete protected exchange. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".private", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted private exchange");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete private exchange. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".notification", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted notification exchange");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete notification exchange. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".publish", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted publish exchange");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete publish exchange. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.exchangeDelete(id+".diagnostics", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted diagnostics exchange");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete diagnostics exchange. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
						
					//delete queues
					adminChannel.queueDelete(id, result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted regular queue");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete regular queue. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".private", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted private queue");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete private queue. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".priority", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted priority queue");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete priority queue. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".command", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted command queue");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete command queue. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
					
					adminChannel.queueDelete(id+".notification", result -> {
						
						if(result.succeeded())
						{
							logger.debug("Deleted notification queue");
							count--;
							if(count==0) resultHandler.handle(Future.succeededFuture());
						}
						else
						{
							logger.debug("Could not delete notification queue. Cause="+result.cause());
							resultHandler.handle(Future.failedFuture(result.cause()));
						}
					});
				}
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
		return this;
	}
	
	@Override
	public BrokerService create_entity_bindings(String id, Handler<AsyncResult<Void>> resultHandler) 
	{
		logger.debug("In create entity bindings");
		
		count 		= 2;
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel
				.queueBind(id+".notification", id+".notification", "#", 
				result -> 
				{
					if(result.succeeded())
					{
						logger.debug("Bound notification queue to notification exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Notification bind failed. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
				
				adminChannel
				.queueBind(id+".private", id+".private", "#", 
				result -> 
				{
					if(result.succeeded())
					{
						logger.debug("Bound private queue to private exchange");
						count--;
						if(count==0) resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Private bind failed. Cause="+result.cause());
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
		
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
		return this;
	}

	@Override
	public BrokerService bind(String queue, String exchange, String routingKey,
	Handler<AsyncResult<Void>> resultHandler) 
	{
		logger.debug("In bind");
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel.queueBind(queue, exchange, routingKey, queueBind -> {
					
					if(queueBind.succeeded())
					{
						logger.debug("Bound "+queue+" to "+exchange+" with "+routingKey);
						resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Bind failed for "+queue+" to "+exchange+" with "+routingKey);
						resultHandler.handle(Future.failedFuture("Error while binding"));
					}
				});
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});

		return this;
	}

	@Override
	public BrokerService publish(String exchange, String routingKey, JsonObject message,
	Handler<AsyncResult<Void>> resultHandler) 
	{
		logger.debug("In publish");
		
		Future<Void>	channel	=	getAdminChannel();
		
		channel.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				logger.debug("Got admin channel");
				
				RabbitMQClient adminChannel = adminpool.get("admin");
				
				adminChannel.basicPublish(exchange, routingKey, message, publish -> {
					
					if(publish.succeeded())
					{
						logger.debug("Published message ="+message.toString());
						resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						logger.debug("Failed to publish message ="+message.toString());
						resultHandler.handle(Future.failedFuture(publish.cause()));
					}
				});
			}
			else
			{
				logger.debug("Could not get admin channel. Cause="+ar.cause());
				resultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
		
		return this;
	}
}
