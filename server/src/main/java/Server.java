import common.dto.*;
import common.messages.*;
import common.requests.*;
import common.responses.*;

import java.net.*;
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.sql.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public final class Server {

	public static final int BASIC_PORT = 43500;

	private ServerSocket serverSocket;
	private final List<ClientHandler> activeClients = new CopyOnWriteArrayList<>();
	private final Map<User, Integer> users = new ConcurrentHashMap<>();
	private final Map<User, BlockingQueue<Message>> messageQueues = new ConcurrentHashMap<>();

	private Connection connection;
	private int serverId = -1;

	private boolean running;
	private boolean corrupted;

	private class ClientHandler extends Thread {
		private final Socket clientSocket;
		private User client = null;
		private boolean isAuthorised = false;

		ObjectOutputStream out;
		ObjectInputStream in;
		public ClientHandler(Socket socket) {
			this.clientSocket = socket;
		}

		public void run() {
			try {
				out = new ObjectOutputStream(clientSocket.getOutputStream());
				try {
					in = new ObjectInputStream(clientSocket.getInputStream());
					while (clientSocket.isConnected()) {
						Object dto = in.readObject();
						if (dto instanceof RegistrationRequest(String name, String passwd)) {
							boolean res = registerUser(name, passwd);
							out.writeObject(new AuthorisationResponse(res));
							if (res) {
								authorise(name);
							}
							out.flush();
						} else if (dto instanceof AuthorisationRequest(String name, String passwd)) {
							int res = authoriseUser(name, passwd);
							out.writeObject(new AuthorisationResponse(res != -1));
							if (res != -1) {
								authorise(name);
							}
							out.flush();
						} else if (dto instanceof GetNamesRequest) {
							if (isAuthorised) {
								out.writeObject(
										new GetNamesResponse(
												activeClients.stream()
														.map(ClientHandler::getClient)
														.filter(Objects::nonNull)
														.filter(u -> u != this.client)
														.collect(Collectors.toList())

										)
								);
							} else {
								out.writeObject(new ErrorMessage("Error: User not authorised"));
							}
							out.flush();
						} else if (dto instanceof GetAllMessagesRequest(MessageSeenState state)) {
							if (isAuthorised) {
								out.writeObject(new GetMessagesResponse(getMessages(client, state)));
							} else {
								out.writeObject(new ErrorMessage("Error: User not authorised"));
							}
							out.flush();
						} else if (dto instanceof GetMessagesRequest(User from, MessageSeenState state)) {
							if (isAuthorised) {
								out.writeObject(new GetMessagesResponse(getMessages(from, client, state)));
							} else {
								out.writeObject(new ErrorMessage("Error: User not authorised"));
							}
							out.flush();
						} else if (dto instanceof SendMessageRequest(MessageContent content, User to)) {
							if (isAuthorised) {
								if (to != null && !users.containsKey(to)) {
									out.writeObject(new ErrorMessage("Recipient not found: " + to.name()));
								} else {
									if (sendMessage(content, client, to, LocalDateTime.now()))
										out.writeObject(new InfoMessage("Message sent"));
									else
										out.writeObject(new ErrorMessage("Message hasn't been sent: internal error"));
								}
							} else {
								out.writeObject(new ErrorMessage("Error: User not authorised"));
							}
							out.flush();
						}else if (dto instanceof GetDialogRequest(User withUser)) {
							if (!isAuthorised) {
								out.writeObject(new ErrorMessage("Error: User not authorised"));
							} else {
								List<Message> dialog = getDialog(client, withUser);
								out.writeObject(new GetMessagesResponse(dialog));
							}
							out.flush();
						}else if (dto instanceof PollRequest(User user)) {
							if (user == null || !messageQueues.containsKey(user)) {
								out.writeObject(new ErrorMessage("Error: Invalid poll request for user: " + user));
							} else {
								Message msg;
								try {
									msg = pollMessage(user, 30, TimeUnit.SECONDS);
								} catch (InterruptedException e) {
									Thread.currentThread().interrupt();
									break;
								}
								if (msg != null) {
									out.writeObject(new GetMessagesResponse(List.of(msg)));
								} else {
									out.writeObject(new GetMessagesResponse(List.of()));
								}
							}
							out.flush();
						} else if (dto instanceof MarkAsSeenRequest(long messageId)) {
							if (!isAuthorised) {
								out.writeObject(new ErrorMessage("Error: User not authorised"));
							} else {
								markMessageAsSeen(messageId);
								out.writeObject(new InfoMessage("Message marked as seen"));
							}
							out.flush();
						} else if (dto instanceof LogoutRequest) {
							out.writeObject(new InfoMessage("Logout successful"));
							out.flush();
							out.close();
							in.close();
							break;
						}
					}
				} catch (EOFException | SocketException e) {
					System.out.println("Client disconnected: " + (client != null ? client.name() : "unknown"));
				} catch (IOException | ClassNotFoundException e) {
					System.err.println("Unexpected error in client handler: " + e.getMessage());
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					if(out != null)
						out.close();
					if(in != null)
						in.close();
				} catch (IOException e) {
					System.err.println("In/Out streams failed to close: " + e.getMessage());
				}

				try { if (!clientSocket.isClosed()) clientSocket.close(); } catch (IOException ignored) {}
				if (client != null) {
					removeQueueForUser(client);
					activeClients.remove(this);
				}
			}
		}

		User getClient(){
			return this.client;
		}

		public void markAsSeen(long messageId){
			if(out != null){
				try {
					out.writeObject(new MarkAsSeenRequest(messageId));
					out.flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		private void authorise(String name){
			if(name != null){
				client = new User(name);
				isAuthorised = true;
				addQueueForUser(client);
			}
		}

		public void closeConnection(){
			try {
				if(!clientSocket.isClosed())
					clientSocket.close();
			} catch (IOException e) {
				//ignore
			}
		}
	}

	public Server(){
		running = false;
		dbConnect("jdbc:postgresql://localhost:5432/proglab4", "postgres", "password");
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (running) stop();
		}));
	}
	private Thread main;
	public boolean start(int port){
		if(running || corrupted)
			return false;
		try{
			serverSocket = new ServerSocket(port);
			registerServer(port);
			running = true;
			main = new Thread(() -> {
				while (running && !serverSocket.isClosed()) {
					try {
						activeClients.add(new ClientHandler(serverSocket.accept()));
						activeClients.getLast().start();
					} catch (SocketException e) {
						if (running) {
							System.err.println("Socket closed unexpectedly: " + e.getMessage());
						}
						break;
					} catch (IOException e) {
						System.err.println("Couldn't accept new client.\nUnexpected error occurred: " + e.getMessage());
					}
				}
			});
			main.start();
			return true;

		} catch (IOException | SQLException e){
			System.err.println("Couldn't start server on port: " + port + ".\nUnexpected error occurred: " + e.getMessage());
			return false;
		}
	}

	public boolean start(){
		return start(BASIC_PORT);
	}

	public void dbConnect(String url, String user, String password){
		corrupted = false;
		try {
			connection = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			System.err.println("Couldn't connect to DB " + url + ", as user " + user + ".\nUnexpected error occurred: " + e.getMessage());
			corrupted = true;
		}
	}

	public boolean stop(){
		if(!running || corrupted)
			return false;

		for(var client : activeClients){
			client.closeConnection();
		}

		running = false;
		try {
			serverSocket.close();
			if(main != null)
				try {
					main.join(2000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
		} catch (IOException e) {
			System.err.println("Couldn't stop server.\nUnexpected error occurred: " + e.getMessage());
			return false;
		}


		activeClients.clear();
		return true;
	}

	private void registerServer(int port) throws SQLException {
		String localIp = getLocalIpAddress();
		String sql = "INSERT INTO servers (serverIp, port) VALUES (?::inet, ?) " +
				"ON CONFLICT (serverIp, port) " +
				"DO UPDATE SET serverIp = EXCLUDED.serverIp, port = EXCLUDED.port RETURNING serverId";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, localIp);
			stmt.setInt(2, port);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				this.serverId = rs.getInt("serverId");
				try(PreparedStatement stmt1 = connection.prepareStatement(
						"select username, userId from users where serverId = ?")
				){
					stmt1.setInt(1, serverId);
					ResultSet rs1 = stmt1.executeQuery();
					while(rs1.next()){
						users.put(new User(rs1.getString("username")), rs1.getInt("userId"));
					}
				}
			} else {
				throw new SQLException("Failed to get serverId");
			}
		}
	}

	private String getLocalIpAddress() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return "127.0.0.1";
		}
	}

	private boolean registerUser(String name, String passwd){
		String sql = "insert into users (serverId, username, passwd) values (?, ?, crypt(?, gen_salt('bf')))";
		try(PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)){
			stmt.setInt(1, serverId);
			stmt.setString(2, name);
			stmt.setString(3, passwd);
			int affected = stmt.executeUpdate();
			if(affected > 0){
				ResultSet rs = stmt.getGeneratedKeys();
				if(rs.next())
					users.put(new User(name), rs.getInt(1));
				return true;
			}
		} catch (SQLException e) {
			if (e.getSQLState().equals("23505")) return false; // duplicate username
			System.err.println("Couldn't register user " + name + ".\nUnexpected error occurred: " + e.getMessage());
		}
		return false;
	}

	private int authoriseUser(String name, String passwd){
		String sql = "select userId from users where serverId = ? and username = ? and passwd = crypt(?, passwd)";
		try(PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, serverId);
			stmt.setString(2, name);
			stmt.setString(3, passwd);
			ResultSet rs = stmt.executeQuery();

			if(rs.next()){
				int userId = rs.getInt("userId");
				users.put(new User(name), userId);
				return userId;
			}
		}catch (SQLException e) {
			System.err.println("Couldn't authorise user " + name + ".\nUnexpected error occurred: " + e.getMessage());
		}
		return -1;
	}

	private String getSqlMessages(MessageSeenState state, User from){
		String sql = "select " +
				"m.messageId, m.content, fromU.username as fromName, toU.username as toName, " +
				"m.dispatchTime, m.seenTime " +
				"from messages m " +
				"left join users fromU on m.fromUser = fromU.userId " +
				"left join users toU on m.toUser = toU.userId " +
				"where ";
		sql += " (m.toUser = ? or m.toUser is null) ";

		if(from != null)
			sql += " and m.fromUser = ? ";
		if(state == MessageSeenState.SEEN)
			sql += " and m.seenTime is not null ";
		else if(state == MessageSeenState.UNCHECKED)
			sql += " and m.seenTime is null ";
		sql += "order by m.dispatchTime";
		return sql;
	}

	private Message resultSetToMessage(ResultSet rs) throws SQLException{
		long id = rs.getLong("messageId");
		String contentStr = rs.getString("content");
		String fromName = rs.getString("fromName");
		String toName = rs.getString("toName");
		LocalDateTime dispatch = rs.getTimestamp("dispatchTime").toLocalDateTime();
		Timestamp seenTs = rs.getTimestamp("seenTime");
		LocalDateTime seen = seenTs != null ? seenTs.toLocalDateTime() : null;
		MessageContent content = new MessageContent(contentStr);
		User from = new User(fromName);
		User to = toName != null ? new User(toName) : null;
		return new Message(id, content, from, to, dispatch, seen);
	}

	private List<Message> getMessages(User to, MessageSeenState state){
		return getMessages(null, to, state);
	}

	private List<Message> getMessages(User from, User to, MessageSeenState state){
		List<Message> resultList = new ArrayList<>();
		String sql = getSqlMessages(state, from);
		try (PreparedStatement stmt = connection.prepareStatement(sql)){
			Integer toId = users.get(to);
			if(toId != null)
				stmt.setInt(1, toId);
			else{
				System.err.println("Couldn't get authorised user " + to.name() + ".");
				return resultList;
			}
			if(from != null){
				Integer fromId = users.get(from);
				if(fromId != null)
					stmt.setInt(2, fromId);
				else{
					System.err.println("Couldn't get authorised user " + from.name() + ".");
					return resultList;
				}
			}
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				resultList.add(resultSetToMessage(rs));
			}
		} catch (SQLException e) {
			System.err.println("Couldn't get messages sent to user " + to.name() + ".\nUnexpected error occurred: " + e.getMessage());
		}
		return resultList;
	}

	public void addQueueForUser(User user) {
		messageQueues.putIfAbsent(user, new LinkedBlockingQueue<>());
	}

	public void removeQueueForUser(User user) {
		messageQueues.remove(user);
	}

	public void deliverMessage(Message msg) {
		User recipient = msg.to();
		if (recipient == null) {
			for (ClientHandler ch : activeClients) {
				User u = ch.getClient();
				if (u != null && !u.equals(msg.from())) {
					BlockingQueue<Message> q = messageQueues.get(u);
					if (q != null) {
						if(!q.offer(msg))
							System.err.println("Couldn't offer a message");
					}
				}
			}
		} else {
			BlockingQueue<Message> q = messageQueues.get(recipient);
			if (q != null)
				if(!q.offer(msg))
					System.err.println("Couldn't offer a message");
		}
	}

	public Message pollMessage(User user, long timeout, TimeUnit unit) throws InterruptedException {
		BlockingQueue<Message> q = messageQueues.get(user);
		return q != null ? q.poll(timeout, unit) : null;
	}

	private boolean sendMessage(MessageContent content, User from, User to, LocalDateTime dispatchTime) {
		String sql = "INSERT INTO messages (content, fromUser, toUser, dispatchTime) VALUES (?, ?, ?, ?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			Integer fromId = users.get(from);
			Integer toId = to == null ? null : users.get(to);

			if (toId == null && to != null) {
				System.err.println("Recipient not found: " + to.name());
				return false;
			}
			if (fromId == null) {
				System.err.println("Sender not found: " + from.name());
				return false;
			}

			stmt.setString(1, content.content());
			stmt.setInt(2, fromId);
			if (toId != null)
				stmt.setInt(3, toId);
			else
				stmt.setNull(3, Types.INTEGER);
			stmt.setTimestamp(4, Timestamp.valueOf(dispatchTime));

			int affected = stmt.executeUpdate();
			if (affected == 1) {
				ResultSet rs = stmt.getGeneratedKeys();
				if (rs.next()) {
					long messageId = rs.getLong(1);
					deliverMessage(new Message(messageId, content, from, to, dispatchTime, null));
					return true;
				}
			}
		} catch (SQLException e) {
			System.err.println("Couldn't send message from " + from.name()
					+ " to " + (to == null ? "(broadcast)" : to.name())
					+ ".\nUnexpected error occurred: " + e.getMessage());
		}
		return false;
	}


	private void markMessageAsSeen(long messageId){
		String sql = "update messages set seenTime = current_timestamp where messageId = ?";
		try(PreparedStatement stmt = connection.prepareStatement(sql)){
			stmt.setLong(1, messageId);
			stmt.executeUpdate();
		}catch(SQLException e){
			System.err.println("Couldn't mark message " + messageId + " as seen: " + e.getMessage());
		}
	}

	private List<Message> getDialog(User me, User with) {
		List<Message> result = new ArrayList<>();
		String sql = "SELECT m.messageId, m.content, fromU.username AS fromName, toU.username AS toName, " +
				"m.dispatchTime, m.seenTime " +
				"FROM messages m " +
				"LEFT JOIN users fromU ON m.fromUser = fromU.userId " +
				"LEFT JOIN users toU ON m.toUser = toU.userId " +
				"WHERE (fromU.username = ? AND toU.username = ?) " +
				"   OR (fromU.username = ? AND toU.username = ?) " +
				"ORDER BY m.dispatchTime";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, me.name());
			stmt.setString(2, with.name());
			stmt.setString(3, with.name());
			stmt.setString(4, me.name());
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				result.add(resultSetToMessage(rs));
			}
		} catch (SQLException e) {
			System.err.println("Couldn't get dialog between " + me.name() + " and " + with.name() + ": " + e.getMessage());
		}
		return result;
	}

	static void main(){
		Server server = new Server();
		Scanner scanner = new Scanner(System.in);
		int choice;

		do {
			System.out.println("\n-SERVER MENU-");
			System.out.println("1. Start");
			System.out.println("2. Start on port");
			System.out.println("3. Stop");
			System.out.println("0. Ext");
			System.out.print("Choice: ");

			while (!scanner.hasNextInt()) {
				System.out.print("Input must be integer");
				scanner.next();
			}
			choice = scanner.nextInt();

			switch (choice) {
				case 1:
					if(server.start())
						System.out.println("Server stared successfully");
					break;
				case 2:
					while (!scanner.hasNextInt()) {
						System.out.print("Input must be integer");
						scanner.next();
					}
					int port = scanner.nextInt();
					if(server.start(port))
						System.out.println("Server stared successfully");
					break;
				case 3:
					if(server.stop())
						System.out.println("Server stopped successfully");
					break;
				case 0:
					server.stop();
					scanner.close();
					return;
				default:
					System.out.println("Invalid choice");
					break;
			}
		} while (true);
	}
}