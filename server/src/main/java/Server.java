import common.Logger;
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
	private final Map<User, Integer> userIdMap = new ConcurrentHashMap<>();
	private final Map<Integer, User> userNameMap = new ConcurrentHashMap<>();
	private final Map<User, BlockingQueue<Message>> messageQueues = new ConcurrentHashMap<>();

	private final Logger logger = new Logger();

	public String showLogs(){
		return logger.showLogs();
	}

	public void clearLogs(){
		logger.clearLogs();
	}

	private Connection connection;
	private int serverId = -1;

	private boolean running;
	private boolean corrupted;

	private class ClientHandler extends Thread {
		private final Socket clientSocket;
		private User client = null;
		private boolean authorised = false;

		ObjectOutputStream out;
		ObjectInputStream in;

		public ClientHandler(Socket socket) {
			this.clientSocket = socket;
			setDaemon(true);
		}

		@Override
		public void run() {
			try {
				out = new ObjectOutputStream(clientSocket.getOutputStream());
				in = new ObjectInputStream(clientSocket.getInputStream());

				while (!clientSocket.isClosed() && !Thread.currentThread().isInterrupted()) {
					Object request = in.readObject();
					handle(request);
				}
			} catch (EOFException | SocketException e) {
				logger.logInfo("Client disconnected: " + (client != null ? client.name() : "unknown"));
			} catch (IOException | ClassNotFoundException e) {
				logger.logError("Unexpected error in client handler: ", e.getMessage());
			} finally {
				logout();
			}
		}

		private void handle(Object request) throws IOException {
			switch (request) {
				case RegistrationRequest(String name, String passwd) -> register(name, passwd);
				case AuthorisationRequest(String name, String passwd) -> authorise(name, passwd);
				case GetNamesRequest() -> getNames();
				case GetDialogRequest(User with) -> handleDialog(with);
				case GetMessagesRequest(User from, MessageSeenState state) -> handleGetMessages(from, state);
				case GetAllMessagesRequest(MessageSeenState state) -> handleGetAllMessages(state);
				case SendMessageRequest(MessageContent content, User to) -> handleSend(content, to);
				case MarkAsSeenRequest(long messageId) -> handleMark(messageId);
				case PollRequest(User user) -> handlePoll(user);
				case LogoutRequest() -> logout();
				default -> logger.logError("Unexpected request type");
			}
			out.flush();
		}

		private void register(String name, String passwd) throws IOException {
			boolean isOk = registerUser(name, passwd);
			out.writeObject(new AuthorisationResponse(isOk));
			if (isOk) authoriseAsClient(name);
		}

		private void authorise(String name, String passwd) throws IOException {
			int userId = authoriseUser(name, passwd);
			out.writeObject(new AuthorisationResponse(userId != -1));
			if (userId != -1) {
				logger.logInfo("User authorised: " + name);
				authoriseAsClient(name);
			}
		}

		private void getNames() throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			out.writeObject(
					new GetNamesResponse(
							activeClients.stream()
									.map(ClientHandler::getClient)
									.filter(Objects::nonNull)
									.filter(u -> u != this.client)
									.collect(Collectors.toList())
					)
			);
		}

		private void handleDialog(User with) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			List<Message> dialog = getDialog(client, with);
			out.writeObject(new GetMessagesResponse(dialog));
		}

		private void handleGetMessages(User from, MessageSeenState state) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			out.writeObject(new GetMessagesResponse(getMessages(from, client, state)));
		}

		private void handleGetAllMessages(MessageSeenState state) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			out.writeObject(new GetMessagesResponse(getMessages(client, state)));
		}

		private void handleSend(MessageContent content, User to) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			if (to != null && !userIdMap.containsKey(to)) {
				sendError("Recipient not found: " + to.name());
				return;
			}

			Integer mId = sendMessage(content, client, to, LocalDateTime.now());
			if (mId != null)
				out.writeObject(new SendMessageResponse(mId));
			else
				sendError("Message hasn't been sent: internal error");
		}

		private void handlePoll(User user) throws IOException {
			if (user == null || !messageQueues.containsKey(user)) {
				sendError("Invalid poll request for user: " + user);
				return;
			}
			try {
				Message msg = messageQueues.get(user).poll(30, TimeUnit.SECONDS);
				out.writeObject(new GetMessagesResponse(msg != null ? List.of(msg) : List.of()));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				sendError("Poll interrupted");
			}
		}

		private void handleMark(long messageId) throws IOException {
			if (!authorised) {
				sendError("User not authorised");
				return;
			}
			Message updated = markMessageAsSeen(messageId);
			if (updated != null) {
				BlockingQueue<Message> senderQueue = messageQueues.get(updated.from());
				if (senderQueue != null) {
					if(!senderQueue.offer(updated))
						logger.logError("SenderQueue error");
				}
				out.writeObject(new InfoMessage("Message marked as seen"));
				var chatter = activeClients.stream().filter(cl -> cl.client == updated.from()).findAny();
				chatter.ifPresent(clientHandler ->
						clientHandler.sendMarkResponse(new MarkAsSeenResponse(updated.messageId(), updated.seenTime())));
			} else {
				out.writeObject(new GetMessagesResponse(List.of()));
			}
		}

		private void logout(){
			try { if (in != null) in.close(); } catch (IOException ignored) {}
			try { if (out != null) out.close(); } catch (IOException ignored) {}
			try { if (!clientSocket.isClosed()) clientSocket.close(); } catch (IOException ignored) {}
			if (client != null) {
				removeQueueForUser(client);
				activeClients.remove(this);
				logger.logInfo("User disconnected: " + client.name());
			}
		}

		public void sendError(String err) throws IOException {
			out.writeObject(new ErrorMessage(err));
		}

		public void sendInfo(String info) throws IOException {
			out.writeObject(new InfoMessage(info));
		}

		public User getClient() {
			return this.client;
		}

		private void authoriseAsClient(String name) {
			if (name != null && !name.isEmpty()) {
				client = new User(name);
				authorised = true;
				addQueueForUser(client);
			}
		}

		public void sendMarkResponse(MarkAsSeenResponse markAsSeenResponse){
			if(!clientSocket.isClosed() && out != null){

				try {
					out.writeObject(markAsSeenResponse);
					out.flush();
				} catch (IOException e) {
					logger.logError("Couldn't send mark response to " + client, e.getMessage());
				}
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
						var ch = new ClientHandler(serverSocket.accept());
						activeClients.add(ch);
						ch.start();
					} catch (SocketException e) {
						if (running) {
							logger.logError("Socket closed unexpectedly", e.getMessage());
						}
						break;
					} catch (IOException e) {
						logger.logError("Couldn't accept new client.\nUnexpected error occurred", e.getMessage());
					}
				}
			});
			main.start();
			return true;

		} catch (IOException | SQLException e){
			logger.logError("Couldn't start server on port: " + port + ".\nUnexpected error occurred", e.getMessage());
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
			logger.logError("Couldn't connect to DB " + url + ", as user " + user + ".\nUnexpected error occurred", e.getMessage());
			corrupted = true;
		}
	}

	public boolean stop(){
		if(!running || corrupted)
			return false;

		for(var client : activeClients){
			try {
				client.sendInfo("Server is closing");
			} catch (IOException e) {
				logger.logError("Server closing: Error occurred when sending info message", e.getMessage());
			}
			client.logout();
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
			logger.logError("Couldn't stop server.\nUnexpected error occurred", e.getMessage());
			return false;
		}
		activeClients.clear();
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
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
			try(ResultSet rs = stmt.executeQuery()){
				if (rs.next()) {
					this.serverId = rs.getInt("serverId");
					try(PreparedStatement stmt1 = connection.prepareStatement(
							"select username, userId from users where serverId = ?")
					){
						stmt1.setInt(1, serverId);
						try(ResultSet rs1 = stmt1.executeQuery()){
							while(rs1.next()){
								var user = new User(rs1.getString("username"));
								int userId = rs1.getInt("userId");
								userIdMap.put(user, userId);
								userNameMap.put(userId, user);
							}
						}
					}
				} else {
					throw new SQLException("Failed to get serverId");
				}
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
				try(ResultSet rs = stmt.getGeneratedKeys()){
					if(rs.next()){
						var user = new User(name);
						userIdMap.put(user, rs.getInt(1));
						userNameMap.put(rs.getInt(1), user);
					}
					return true;
				}
			}
		} catch (SQLException e) {
			if (e.getSQLState().equals("23505")) return false; // duplicate username
			logger.logError("Couldn't register user " + name + ".\nUnexpected error occurred", e.getMessage());
		}
		return false;
	}

	private int authoriseUser(String name, String passwd){
		String sql = "select userId from users where serverId = ? and username = ? and passwd = crypt(?, passwd)";
		try(PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, serverId);
			stmt.setString(2, name);
			stmt.setString(3, passwd);
			try(ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					int userId = rs.getInt("userId");
					userIdMap.put(new User(name), userId);
					userNameMap.put(userId, new User(name));
					return userId;
				}
			}
		}catch (SQLException e) {
			logger.logError("Couldn't authorise user " + name + ".\nUnexpected error occurred", e.getMessage());
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
			Integer toId = userIdMap.get(to);
			if(toId != null)
				stmt.setInt(1, toId);
			else{
				logger.logError("Couldn't get authorised user " + to.name() + ".");
				return resultList;
			}
			if(from != null){
				Integer fromId = userIdMap.get(from);
				if(fromId != null)
					stmt.setInt(2, fromId);
				else{
					logger.logError("Couldn't get authorised user " + from.name() + ".");
					return resultList;
				}
			}
			try(ResultSet rs = stmt.executeQuery()){
				while(rs.next()){
					resultList.add(resultSetToMessage(rs));
				}
			}
		} catch (SQLException e) {
			logger.logError("Couldn't get messages sent to user " + to.name() + ".\nUnexpected error occurred", e.getMessage());
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
							logger.logError("Couldn't offer a message");
					}
				}
			}
		} else {
			BlockingQueue<Message> q = messageQueues.get(recipient);
			if (q != null)
				if(!q.offer(msg))
					logger.logError("Couldn't offer a message");
		}
	}

	private Integer sendMessage(MessageContent content, User from, User to, LocalDateTime dispatchTime) {
		String sql = "INSERT INTO messages (content, fromUser, toUser, dispatchTime) VALUES (?, ?, ?, ?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			Integer fromId = userIdMap.get(from);
			Integer toId = to == null ? null : userIdMap.get(to);

			if (toId == null && to != null) {
				logger.logError("Recipient not found: " + to.name());
				return null;
			}
			if (fromId == null) {
				logger.logError("Sender not found: " + from.name());
				return null;
			}

			stmt.setString(1, content.content());
			stmt.setInt(2, fromId);
			if (toId != null)
				stmt.setInt(3, toId);
			else
				stmt.setNull(3, Types.INTEGER);
			stmt.setTimestamp(4, Timestamp.valueOf(dispatchTime));

			int affected = stmt.executeUpdate();
			logger.logInfo("Message from " + from.name() +
					" to " + (to == null ? "(broadcast)" : to.name()) + " has been sent");
			if (affected == 1) {
				try(ResultSet rs = stmt.getGeneratedKeys()){
					if (rs.next()) {
						int messageId = rs.getInt(1);
						deliverMessage(new Message(messageId, content, from, to, dispatchTime, null));
						logger.logInfo("Message from " + from.name() +
								" to " + (to == null ? "(broadcast)" : to.name()) +
								" with " + messageId + " has been delivered");
						return messageId;
					}
				}
			}
		} catch (SQLException e) {
			logger.logError("Couldn't send message from " + from.name()
					+ " to " + (to == null ? "(broadcast)" : to.name())
					+ ".\nUnexpected error occurred", e.getMessage());
		}
		return null;
	}

	private Message markMessageAsSeen(long messageId){
		String sql = "update messages set seenTime = ? where messageId = ? returning *";
		try(PreparedStatement stmt = connection.prepareStatement(sql)){
			stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
			stmt.setLong(2, messageId);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					String content = rs.getString("content");
					User from = userNameMap.get(rs.getInt("fromUser"));
					User to = userNameMap.get(rs.getInt("toUser"));
					LocalDateTime dispatchTime = rs.getTimestamp("dispatchTime").toLocalDateTime();
					LocalDateTime seenTime = rs.getTimestamp("seenTime").toLocalDateTime();
					return new Message(messageId, new MessageContent(content), from, to, dispatchTime, seenTime);
				} else {
					logger.logError("Message with id: " + messageId + " not found");
				}
			}
		}catch(SQLException e){
			logger.logError("Couldn't mark message " + messageId + " as seen", e.getMessage());
		}
		return null;
	}

	private List<Message> getDialog(User me, User with) {
		List<Message> result = new ArrayList<>();
		String sql = "SELECT m.messageId, m.content, fromU.username AS fromName, toU.username AS toName, " +
				"m.dispatchTime, m.seenTime " +
				"FROM messages m " +
				"LEFT JOIN users fromU ON m.fromUser = fromU.userId " +
				"LEFT JOIN users toU ON m.toUser = toU.userId " +
				"WHERE (fromU.username = ? AND (toU.username = ? OR m.toUser is null)) " +
				"   OR (fromU.username = ? AND (toU.username = ? OR m.toUser is null)) " +
				"ORDER BY m.dispatchTime";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, me.name());
			stmt.setString(2, with.name());
			stmt.setString(3, with.name());
			stmt.setString(4, me.name());
			try(ResultSet rs = stmt.executeQuery()){
				while (rs.next()) {
					result.add(resultSetToMessage(rs));
				}
			}
		} catch (SQLException e) {
			logger.logError("Couldn't get dialog between " + me.name() + " and " + with.name(), e.getMessage());
		}
		return result;
	}

	static void main(){
		Server server = new Server();
		Scanner scanner = new Scanner(System.in);
		int choice;

		do {
			System.out.println("-SERVER MENU-");
			System.out.println("1. Start");
			System.out.println("2. Start on port");
			System.out.println("3. Stop");
			System.out.println("4. Show logs");
			System.out.println("5. Clear logs");
			System.out.println("0. Ext");
			System.out.print("Act: ");

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
				case 4:
					System.out.println("-=LOGS=-");
					System.out.println(server.showLogs());
					break;
				case 5:
					server.clearLogs();
					System.out.println("Cleared");
					break;
				case 6:
					server.logger.clearInfo();
					break;
				case 7:
					server.logger.clearErrors();
					break;
				case 0:
					server.stop();
					scanner.close();
					return;
				default:
					System.out.println("Invalid choice");
					break;
			}
			System.out.print("\n");
		} while (true);
	}
}