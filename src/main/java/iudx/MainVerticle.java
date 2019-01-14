package iudx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.DeploymentOptions;

public class MainVerticle extends AbstractVerticle
{	
	@Override
	public void start(Future<Void> startFuture)throws Exception
	{
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
		   
		   System.out.println(name);
		   
		   vertx.deployVerticle(name, res -> 
		   {
		      if(res.failed())
		      {
		         System.out.println("Failed to deploy verticle " + name);
		         future.fail(res.cause());
		      } 
		      else 
		      {
		    	 System.out.println("Deployed verticle " + name);
		         future.complete();
		      }
		   });
		   
		   return future;
		}
}
