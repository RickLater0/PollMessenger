package DAO;

import common.dto.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDAO {

	public static User getById(int id){

		Connection connection;
		try {
			connection = DAO_Conf.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		var serverId = DAO_Conf.serverId;

		String sql = "select * from users where id = ? AND serverId = ?";
		User user = null;
		try(PreparedStatement stmt = connection.prepareStatement(sql)){
			stmt.setLong(1, id);
			stmt.setLong(2, serverId);
			try(ResultSet rs = stmt.executeQuery()){
				if(rs.next()){
					user = new User(rs.getLong("userid"), rs.getString("username"));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return user;
	}

	public static User getByName(String name) throws SQLException {
		Connection connection = DAO_Conf.getConnection();

		var serverId = DAO_Conf.serverId;

		String sql = "select * from users where username = ? AND serverId = ?";
		User user = null;
		try(PreparedStatement stmt = connection.prepareStatement(sql)){
			stmt.setString(1, name);
			stmt.setLong(2, serverId);
			try(ResultSet rs = stmt.executeQuery()){
				if(rs.next()){
					user = new User(rs.getLong("userid"), rs.getString("username"));
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
		String sql = "insert into users (username, passwd, serverid) values (?, crypt(?, gen_salt('bf')), ?)";
		
		try(PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
			stmt.setString(1, name);
			stmt.setString(2, passwd);
			stmt.setLong(3, serverId);

			int affected = stmt.executeUpdate();
			if(affected == 1){
				try (ResultSet rs = stmt.getGeneratedKeys()){
					if(rs.next()){
						user = new User(rs.getLong("userid"), rs.getString("username"));
					}
				}
			}
		}

		return user;
	}

	public static User authorise(String name, String passwd) throws SQLException {
		var connection = DAO_Conf.getConnection();
		var serverId = DAO_Conf.serverId;
		String sql = """
				select
				    userid,
				    username
				from users
				where
				    serverid = ? and
				    username = ? and
				    passwd = crypt(?, passwd)
				""";

		try(PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
			stmt.setLong(1, serverId);
			stmt.setString(2, name);
			stmt.setString(3, passwd);

			try(ResultSet rs = stmt.executeQuery()){
				if(rs.next()){
					return new User(rs.getLong("userid"), rs.getString("username"));
				}
			}
		}

		return null;
	}

	public static List<User> getAll() throws SQLException {
		var connection = DAO_Conf.getConnection();
		var serverId = DAO_Conf.serverId;

		String sql = """
				select
				    *
				from users
				where
				    serverid = ?
				order by
				    username desc
				""";

		try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			stmt.setLong(1, serverId);
			try (ResultSet rs = stmt.executeQuery()) {
				List<User> users = new ArrayList<>();
				while (rs.next()) {
					users.add(new User(rs.getLong("userid"), rs.getString("username")));
				}
				return users;
			}
		}
	}

	public static List<User> getAllExcept(Long exceptId) throws SQLException {
		var connection = DAO_Conf.getConnection();
		var serverId = DAO_Conf.serverId;

		String sql = """
				select
				    *
				from users
				where
				    serverid = ? and
				    userid != ?
				order by
				    username desc
				""";

		try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			stmt.setLong(1, serverId);
			stmt.setLong(2, exceptId);
			try (ResultSet rs = stmt.executeQuery()) {
				List<User> users = new ArrayList<>();
				while (rs.next()) {
					users.add(new User(rs.getLong("userid"), rs.getString("username")));
				}
				return users;
			}
		}
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

			stmt.setLong(paramIndex++, id);
			stmt.setLong(paramIndex, serverId);

			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					return new User(rs.getLong("userid"), rs.getString("username"));
				}
			}

		}

		return null;
	}
}
