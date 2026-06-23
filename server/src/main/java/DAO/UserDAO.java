package DAO;

import common.dto.User;

import java.sql.*;

public class UserDAO {

	public static User getById(int id){

		Connection connection = null;
		try {
			connection = DAO_Conf.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		var serverId = DAO_Conf.serverId;

		String sql = "select * from users where id = ? AND serverId = ?";
		User user = null;
		try(PreparedStatement stmt = connection.prepareStatement(sql)){
			stmt.setInt(1, id);
			stmt.setInt(2, serverId);
			try(ResultSet rs = stmt.executeQuery()){
				if(rs.next()){
					user = new User(rs.getInt("userid"), rs.getString("username"));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return user;
	}

	public static User getByName(String name){

		Connection connection = null;
		try {
			connection = DAO_Conf.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		var serverId = DAO_Conf.serverId;

		String sql = "select * from users where username = ? AND serverId = ?";
		User user = null;
		try(PreparedStatement stmt = connection.prepareStatement(sql)){
			stmt.setString(1, name);
			stmt.setInt(2, serverId);
			try(ResultSet rs = stmt.executeQuery()){
				if(rs.next()){
					user = new User(rs.getInt("userid"), rs.getString("username"));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return user;
	}

	public static User insert(String name, String passwd) throws SQLException {

		var connection = DAO_Conf.getConnection();
		var serverId = DAO_Conf.serverId;
		User user = null;
		String sql = "insert into users (username, password, serverid) values (?, crypt(?, gen_salt('bf')), ?)";
		
		try(PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
			stmt.setString(1, name);
			stmt.setString(2, passwd);
			stmt.setInt(3, serverId);

			int affected = stmt.executeUpdate();
			if(affected == 1){
				try (ResultSet rs = stmt.getGeneratedKeys()){
					if(rs.next()){
						user = new User(rs.getInt("userid"), rs.getString("username"));
					}
				}
			}
		}

		return user;
	}

	public static User update(int id, String name, String passwd) throws SQLException {

		var connection = DAO_Conf.getConnection();
		var serverId = DAO_Conf.serverId;

		boolean updateName = name != null && !name.isBlank();
		boolean updatePasswd = passwd != null && !passwd.isBlank();

		if (!updateName && !updatePasswd) {
			return getById(id);
		}

		StringBuilder sql = new StringBuilder("update users set");
		boolean needComma = false;

		if (updateName) {
			sql.append(" username = ?");
			needComma = true;
		}

		if (updatePasswd) {
			if (needComma) sql.append(",");
			sql.append(" where id = ? AND serverId = ? RETURNING userid, username");
		}
		sql.append(" where id = ? AND serverId = ?");
		try(PreparedStatement stmt = connection.prepareStatement(sql.toString())){
			int paramIndex = 1;

			if (updateName) {
				stmt.setString(paramIndex++, name);
			}

			if (updatePasswd) {
				stmt.setString(paramIndex++, passwd);
			}

			stmt.setInt(paramIndex++, id);
			stmt.setInt(paramIndex++, serverId);

			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					return new User(rs.getInt("userid"), rs.getString("username"));
				}
			}

		}

		return null;
	}
}
