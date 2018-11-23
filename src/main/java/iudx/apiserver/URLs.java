/**
 * 
 */
package iudx.apiserver;

/**
 * @author Swaminathan Vasanth Rajaraman <swaminathanvasanth.r@gmail.com>
 *
 */
public class URLs {
	public static final String broker_url = "localhost";
	public static final String broker_username = "admin";
	public static final String broker_password = "admin@123";
	public static final int broker_port = 5672;
	public static final String broker_vhost = "/";
	public static final String psql_database_url = "localhost";
	public static final String psql_database_username = "postgres";
	public static final String psql_database_password = "admin";
	public static final int psql_database_port = 5432;
	public static final String psql_database_name = "postgres";
		
	public static String getPsqlDatabaseUrl() {
		return psql_database_url;
	}
	public static String getPsqlDatabaseUsername() {
		return psql_database_username;
	}
	public static String getPsqlDatabasePassword() {
		return psql_database_password;
	}
	public static int getPsqlDatabasePort() {
		return psql_database_port;
	}
	public static String getBrokerUrl() {
		return broker_url;
	}
	public static String getBrokerUsername() {
		return broker_username;
	}
	public static String getBrokerPassword() {
		return broker_password;
	}
	public static int getBrokerPort() {
		return broker_port;
	}
	public static String getBrokerVhost() {
		return broker_vhost;
	}
}
