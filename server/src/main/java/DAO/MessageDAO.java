package DAO;

import common.dto.Message;
import common.dto.MessageContent;
import common.dto.MessageSeenState;
import common.dto.User;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageDAO {

	private static Message resultSetToMessage(ResultSet rs) throws SQLException{
		long id = rs.getLong("messageId");
		String contentStr = rs.getString("content");
		String fromName = rs.getString("fromName");
		String toName = rs.getString("toName");
		LocalDateTime dispatch = rs.getTimestamp("dispatchTime").toLocalDateTime();
		Timestamp seenTs = rs.getTimestamp("seenTime");
		LocalDateTime seen = seenTs != null ? seenTs.toLocalDateTime() : null;
		MessageContent content = new MessageContent(contentStr);

		return new Message(id, content, fromName, toName, dispatch, seen);
	}

	public static long insert(
			MessageContent content,
			Long from,
			Long to,
			LocalDateTime dispatchTime
	) throws SQLException {
		String sql = """
			INSERT INTO messages
			    (content, fromUser, toUser, dispatchTime)
			VALUES
			    (?, ?, ?, ?)
			""";
		try (PreparedStatement stmt = DAO_Conf.getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

			stmt.setString(1, content.content());
			stmt.setLong(2, from);
			if (to != null)
				stmt.setLong(3, to);
			else
				stmt.setNull(3, Types.INTEGER);
			stmt.setTimestamp(4, Timestamp.valueOf(dispatchTime));

			int affected = stmt.executeUpdate();

			if (affected == 1) {
				try(ResultSet rs = stmt.getGeneratedKeys()){
					if (rs.next()) return rs.getInt(1);
				}
			}
		}
		return -1;
	}

	public static Map<User, Long> insertBroadcast(
			MessageContent content,
			long fromId,
			LocalDateTime dispatchTime
	) throws SQLException {
		String sql = """
        INSERT INTO messages
            (content, fromUser, toUser, dispatchTime)
        VALUES
            (?, ?, ?, ?)
        """;

		Map<User, Long> generatedIds = new HashMap<>();

		try (PreparedStatement stmt = DAO_Conf.getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

			var targets = UserDAO.getAllExcept(fromId);
			for (var target : targets) {
				stmt.setString(1, content.content());
				stmt.setLong(2, fromId);
				stmt.setLong(3, target.id());
				stmt.setTimestamp(4, Timestamp.valueOf(dispatchTime));
				stmt.addBatch();
			}

			stmt.executeBatch();

			try (ResultSet rs = stmt.getGeneratedKeys()) {
				int i = 0;
				while (rs.next()) {
					generatedIds.put(targets.get(i), rs.getLong(1)); // В PostgreSQL это вернет ID в порядке вставки
					i++;
				}
			}
		}
		return generatedIds;
	}

	public static Message markMessageAsSeen(long messageId) throws SQLException {
		String sql = """
            WITH updated_msg AS (
                UPDATE messages
                SET seenTime = ?
                WHERE messageId = ?
                RETURNING *
            )
            SELECT
                u.messageId,
                u.content,
                fromUserT.username AS fromName,
                toUserT.username AS toName,
                u.dispatchTime,
                u.seenTime
            FROM updated_msg u
            JOIN users fromUserT ON u.fromUser = fromUserT.userId
            LEFT JOIN users toUserT ON u.toUser = toUserT.userId;
            """;

		try (PreparedStatement stmt = DAO_Conf.getConnection().prepareStatement(sql)) {
			stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
			stmt.setLong(2, messageId);

			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					return resultSetToMessage(rs);
				}
			}
		}

		return null;
	}

	public static List<Message> getDialog(long first, long second) throws SQLException {
		String sql = """
		select
			m.messageId,
			m.content,
			fromUsers.username as fromName,
			toUsers.username as toName,
			m.dispatchTime,
			m.seenTime
		from messages m
		inner join users fromUsers on m.fromUser = fromUsers.userId
		left join users toUsers on m.toUser = toUsers.userId
		where
		    ((m.touser = ? and m.fromuser = ?) or
		     (m.touser = ? and m.fromuser = ?))
		order by
		    m.dispatchTime
		;
		""";

		ArrayList<Message> messages = new ArrayList<>();
		try (PreparedStatement stmt = DAO_Conf.getConnection().prepareStatement(sql)) {
			stmt.setLong(1, first);
			stmt.setLong(2, second);
			stmt.setLong(3, second);
			stmt.setLong(4, first);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					messages.add(resultSetToMessage(rs));
				}
			}
		}
		return messages;
	}

	private static String getSqlMessages(MessageSeenState state){
		String sql = """
		select
			m.messageId,
			m.content,
			fromU.username as fromName,
			toU.username as toName,
			m.dispatchTime,
			m.seenTime
		from messages m
		left join users fromU on m.fromUser = fromU.userId
		left join users toU on m.toUser = toU.userId
		where
			m.toUser = ? and
			m.fromUser = ?
		""";

		if(state == MessageSeenState.SEEN)
			sql += " and m.seenTime is not null ";
		else if(state == MessageSeenState.UNCHECKED)
			sql += " and m.seenTime is null ";
		sql += "order by m.dispatchTime";
		return sql;
	}

	public static List<Message> getMessages(User from, User to, MessageSeenState state) throws SQLException {
		String sql = getSqlMessages(state);
		ArrayList<Message> messages = new ArrayList<>();

		try (PreparedStatement stmt = DAO_Conf.getConnection().prepareStatement(sql)){
			Long toId = to.id();
			if(toId != null)
				stmt.setLong(1, toId);
			else
				return null;

			Long fromId = from.id();
			if(fromId != null)
				stmt.setLong(2, fromId);
			else
				return null;

			try(ResultSet rs = stmt.executeQuery()){
				while(rs.next()){
					messages.add(resultSetToMessage(rs));
				}
			}
		}
		return messages;
	}

}
