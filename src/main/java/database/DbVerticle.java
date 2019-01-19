package database;

import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.serviceproxy.ServiceBinder;
import iudx.URLs;

public class DbVerticle extends AbstractVerticle 
{
	public PgPool client;

	@Override
	public void start(Future<Void> startFuture) throws Exception
	{
		PgPoolOptions options = new PgPoolOptions();
        options.setDatabase(URLs.psql_database_name);
        options.setHost(URLs.psql_database_url); 
        options.setPort(URLs.psql_database_port);
        options.setUser(URLs.psql_database_username);
        options.setPassword(URLs.psql_database_password);
        options.setCachePreparedStatements(true);
        options.setMaxSize(10);

		DbService dbService		=	new DbServiceImpl(vertx, options);
		ServiceBinder binder	=	new ServiceBinder(vertx);
		
		binder	
		.setAddress("db.queue")
		.register(DbService.class, dbService);
		
		startFuture.complete();
	}	
}