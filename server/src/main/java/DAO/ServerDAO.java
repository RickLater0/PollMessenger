package DAO;

import common.dto.User;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

//TODO реализовать
public class ServerDAO {
	public static List<User> register(String serverIp, int port) throws SQLException {
		String sql = "INSERT INTO servers (serverIp, port) VALUES (?::inet, ?) " +
				"ON CONFLICT (serverIp, port) " +
				"DO UPDATE SET serverIp = EXCLUDED.serverIp, port = EXCLUDED.port RETURNING serverId";
		try (PreparedStatement stmt = DAO_Conf.getConnection().prepareStatement(sql)) {
			stmt.setString(1, serverIp);
			stmt.setInt(2, port);
			try(ResultSet rs = stmt.executeQuery()){
				if (rs.next()) {
					DAO_Conf.serverId = rs.getInt("serverId");
					try(PreparedStatement stmt1 = DAO_Conf.getConnection().prepareStatement(
							"select username, userId from users where serverId = ?")
					){
						stmt1.setInt(1, DAO_Conf.serverId);
						try(ResultSet rs1 = stmt1.executeQuery()){
							List<User> users = new ArrayList<User>();
							while(rs1.next()){
								var name = rs1.getString("username");
								int userId = rs1.getInt("userId");
								users.add(new User(userId, name));
							}
							return users;
						}
					}
				} else {
					throw new SQLException("Failed to get serverId");
				}
			}
		}

	}


}
