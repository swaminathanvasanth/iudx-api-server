package iudx;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;


public class MainVerticle extends AbstractVerticle
{	
	private final static Logger logger = Logger.getLogger(MainVerticle.class.getName());
	
	@Override
	public void start(Future<Void> startFuture)throws Exception
	{
		logger.setLevel(Level.INFO);
		
		CompositeFuture.all(deployHelper(DbClient.class.getName()), 
							deployHelper(BrokerClient.class.getName()), 
							deployHelper(apiserver.class.getName()).setHandler(ar -> {
								
							if(ar.succeeded())
							{
								startFuture.complete();
							}
							else
							{
								startFuture.fail(ar.cause().toString());
							}
						})
					);
	}
	
	private Future<Void> deployHelper(String name)
	{
		   final Future<Void> future = Future.future();
		   
		   if(name.equals(("iudx.apiserver")))
		   {
			   vertx.deployVerticle(name, new DeploymentOptions()
					   					  .setWorker(true)
					   					  .setInstances(Runtime.getRuntime()
					   					  .availableProcessors()), res -> {
			   if(res.succeeded()) 
			   {
				   logger.info("Deployed Verticle " + name);
				   future.complete();
			   }
			   else
			   {
				   logger.severe("Failed to deploy verticle " + res.cause());
				   future.fail(res.cause());
			   }
					   						  
					   					  									});
		   }
		   else
		   {
			   vertx.deployVerticle(name, res -> 
			   {
			      if(res.failed())
			      {
			         logger.severe("Failed to deploy verticle" + name);
			         future.fail(res.cause());
			      } 
			      else 
			      {
			    	 logger.info("Deployed Verticle " + name);
			         future.complete();
			      }
			   });
		   }
		   
		   return future;
		}
}
