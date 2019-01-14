package main.java.iudx.apiserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

public class BrokerClient extends AbstractVerticle
{
	Map<String, RabbitMQClient> adminpool = new ConcurrentHashMap<String, RabbitMQClient>();
	RabbitMQClient client;
	int count;
	
	@Override
	public void start(Future<Void> startFuture)throws Exception
	{
		Future<Void> init_admin_conn = init_admin_connection();
		
		init_admin_conn.setHandler(ar -> {
			
			if(ar.succeeded())
			{
				vertx.eventBus().consumer("broker.queue", this::onMessage);
				startFuture.complete();
			}
		});
	}
	
	public Future<Void> init_admin_connection()
	{
		Future<Void> init_admin_conn = Future.future();
		
		RabbitMQOptions broker_config = new RabbitMQOptions();
		broker_config.setHost(URLs.getBrokerUrl());
		broker_config.setPort(URLs.getBrokerPort());
		broker_config.setVirtualHost(URLs.getBrokerVhost());
		broker_config.setUser(URLs.getBrokerUsername());
		broker_config.setPassword(URLs.getBrokerPassword());
		broker_config.setConnectionTimeout(6000);
		broker_config.setRequestedHeartbeat(60);
		broker_config.setHandshakeTimeout(6000);
		broker_config.setRequestedChannelMax(5);
		broker_config.setNetworkRecoveryInterval(500);

		client = RabbitMQClient.create(vertx, broker_config);
		client.start(start_handler -> {
			if (start_handler.succeeded()) {
				adminpool.put("admin", client);
				init_admin_conn.complete();

			}
		});
		
		return init_admin_conn;
	}
	
	public RabbitMQClient getAdminChannel()
	{
		if(!adminpool.containsKey("admin"))
		{
			init_admin_connection();
		}
		else if(!adminpool.get("admin").isOpenChannel())
		{
			init_admin_connection();
		}
		
		return adminpool.get("admin");
	}
	
	public void onMessage(Message<JsonObject> message)
	{
		if(!message.headers().contains("action"))
		{
			return;
		}

		String action = message.headers().get("action");

		switch(action)
		{
			case "create-entity-resources":
				create_entity_resources(message);
				break;
			case "create-entity-bindings":
				create_entity_bindings(message);
				break;
			case "create-owner-resources":
				create_owner_resources(message);
				break;
			case "create-owner-bindings":
				create_owner_bindings(message);
				break;
			case "delete-owner-resources":
				delete_owner_resources(message);
				break;
			case "delete-owner-entities":
			case "delete-entity-resources":
				delete_entity_resources(message);
				break;
			
		}
	}
	
	public void create_owner_resources(Message<JsonObject> message)
	{
		count 		= 2;
		String id	= message.body().getString("id");
		
		getAdminChannel().exchangeDeclare(id+".notification", "topic", true, false, 
		result -> 
		{
			if(result.succeeded())
			{
				count--;
				if(count==0)reply_to_create_resource(message);
			}
		});
		
		getAdminChannel().queueDeclare(id+".notification", true, false, false, 
		result -> 
		{
			if(result.succeeded())
			{
				count--;
				if(count==0)reply_to_create_resource(message);
			}
		});
	}
	
	public void delete_owner_resources(Message<JsonObject> message)
	{
		count 		= 2;
		String id	= message.body().getString("id");
		
		System.out.println("owner id = " + id);
		
		getAdminChannel().exchangeDelete(id+".notification", result -> {
			
			if(result.succeeded())
			{
				count--;
				System.out.println("deleted notification exchange");
				if(count == 0) reply_to_delete_resource(message);
			}
		});
		
		getAdminChannel().queueDelete(id+".notification", result -> {
			
			if(result.succeeded())
			{
				count--;
				System.out.println("deleted notification queue");
				if(count == 0) reply_to_delete_resource(message);
			}
		});
	}
	
	public void create_owner_bindings(Message<JsonObject> message)
	{
		count		= 1;
		String id 	= message.body().getString("id");
		
		getAdminChannel().queueBind(id+".notification", id+".notification", "#", 
		result ->
		{
			if(result.succeeded())
			{
				count--;
				if(count==0)reply_to_bind(message);
			}
		});
	}
	
	public void create_entity_resources(Message<JsonObject> message)
	{
		count 		= 11;
		String id 	= message.body().getString("id");
		
		//create exchanges
		getAdminChannel().exchangeDeclare(id+".public", "topic", true, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().exchangeDeclare(id+".protected", "topic", true, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().exchangeDeclare(id+".private", "topic", true, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().exchangeDeclare(id+".notification", "topic", true, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().exchangeDeclare(id+".publish", "topic", true, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().exchangeDeclare(id+".diagnostics", "topic", true, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
				
		//create queues
		getAdminChannel().queueDeclare(id, true, false, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().queueDeclare(id+".private", true, false, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().queueDeclare(id+".priority", true, false, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().queueDeclare(id+".command", true, false, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
		getAdminChannel().queueDeclare(id+".notification", true, false, false, resultHandler -> {
			count--;
			if(count==0) reply_to_create_resource(message);
		});
	}
	
	public void delete_entity_resources(Message<JsonObject> message)
	{
		System.out.println("in delete entity resources");
		String id_list[] 	= message.body().getString("id_list").split(",");
		count 				= (11 * id_list.length);
		
		// TODO: Do not use a plain loop. Use vertx.executeBlocking
		for(String id:id_list)
		{
			if(id == "") continue;
			
			getAdminChannel().exchangeDelete(id+".public", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().exchangeDelete(id+".protected", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().exchangeDelete(id+".private", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().exchangeDelete(id+".notification", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().exchangeDelete(id+".publish", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().exchangeDelete(id+".diagnostics", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
				
			//create queues
			getAdminChannel().queueDelete(id, resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().queueDelete(id+".private", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().queueDelete(id+".priority", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().queueDelete(id+".command", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
			getAdminChannel().queueDelete(id+".notification", resultHandler -> {
				count--;
				if(count == 0) reply_to_delete_resource(message);
			});
		}
	}
	
	public void create_entity_bindings(Message<JsonObject> message)
	{
		count 		= 2;
		String id 	= message.body().getString("id");
		
		getAdminChannel()
		.queueBind(id+".notification", id+".notification", "#", 
		resultHandler -> 
		{
			count--;
			if(count==0) reply_to_bind(message);
		});
		
		getAdminChannel()
		.queueBind(id+".private", id+".private", "#", 
		resultHandler -> 
		{
			count--;
			if(count==0) reply_to_bind(message);
		});
	}
	
	public void reply_to_create_resource(Message<JsonObject> message)
	{
		message.reply(new JsonObject().put("status", "created"));
	}
	public void reply_to_bind(Message<JsonObject> message)
	{
		message.reply(new JsonObject().put("status", "bound"));
	}
	public void reply_to_delete_resource(Message<JsonObject> message)
	{
		message.reply(new JsonObject().put("status", "deleted"));
	}
}
