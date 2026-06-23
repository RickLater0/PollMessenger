package DAO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DAO_Conf {
	private static Connection connection = null;
	public static int serverId = -1;

	public static Connection getConnection() throws SQLException {
		if(connection == null){
			String url =  "jdbc:postgresql://localhost:5432/proglab4";
			String user = "postgres";
			String password = "password";
			connection = DriverManager.getConnection(url, user, password);
		}
		return connection;
	}
}
