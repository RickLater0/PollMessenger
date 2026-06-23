import DAO.DAO_Conf;
import DAO.UserDAO;
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


public final class Server {

	public static final int BASIC_PORT = 43500;

	private ServerSocket serverSocket;
	/**Активные клиенты*/
	private final List<ClientHandler> activeClients = new CopyOnWriteArrayList<>();
	/**Действующие poller-сокеты*/
	private final List<ClientHandler> activePollers = new CopyOnWriteArrayList<>();
	/**Имя-идентификатор в базе для пользователей*/
	private final Map<String, User> usersByName = new ConcurrentHashMap<>();
	/**Идентификатор-имя в базе для пользователей*/
	private final Map<Integer, User> usersById = new ConcurrentHashMap<>();
	/**Очереди сообщений для пользователей*/
	private final Map<String, BlockingQueue<Message>> messageQueues = new ConcurrentHashMap<>();
	//TODO реализовать виртуальные потоки
	private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

	/**Основной инструмент, записывающий логи*/
	private final Logger logger = new Logger();

	public String showLogs(){
		return logger.showLogs();
	}

	public void clearLogs(){
		logger.clearLogs();
	}

	/**Подключение к БД*/
	private Connection connection;
	/**
	 * Идентификатор сервера в БД, нужен для регистрации пользователя
	 * (подключение к конкретному серверу)*/
	private int serverId = -1;

	/**
	 * Отображает статус сервера - запущен или нет*/
	private boolean running;

	/**
	 * Ошибка соединения с БД - сервер ломается*/

	private boolean corrupted;

	/**
	 * Внутренний класс обработчика клиента.
	 * Три состояния: не авторизован, авторизован, авторизован как poller
	 * (состояние зависит от authorised и в какой коллекции лежит сей продукт)
	 * */
	private class ClientHandler extends Thread {
		private final Socket clientSocket;
		private User client = null;
		private boolean authorised = false;

		private final ObjectOutputStream out;
		private final ObjectInputStream in;

		public ClientHandler(Socket socket) throws IOException {
			this.clientSocket = socket;
			out = new ObjectOutputStream(clientSocket.getOutputStream());
			in = new ObjectInputStream(clientSocket.getInputStream());

			setDaemon(true);
		}

		/**Получение объектов общения от клиента
		 * отправка их в дальнейшую обработку*/
		@Override
		public void run() {
			try {
				while (!clientSocket.isClosed() && !Thread.currentThread().isInterrupted()) {
					Object request = in.readObject();
					handle(request);
				}
			} catch (EOFException | SocketException e) {
				logger.logInfo("Client disconnected: " + (client != null ? client : "unknown"));
			} catch (IOException | ClassNotFoundException e) {
				logger.logError("Unexpected error in client handler: ", e.getMessage());
			} finally {
				logout();
			}
		}

		/**Обработка объектов общения-ДТОшек*/
		private void handle(Object request) throws IOException {
			switch (request) {
				case RegistrationRequest(String name, String passwd) -> register(name, passwd);
				case AuthorisationRequest(String name, String passwd) -> authorise(name, passwd);
				case GetNamesRequest() -> getNames();
				case GetDialogRequest(String with) -> handleDialog(with);
				case GetMessagesRequest(User from, MessageSeenState state) -> handleGetMessages(from, state);
				case GetAllMessagesRequest(MessageSeenState state) -> handleGetAllMessages(state);
				case SendMessageRequest(MessageContent content, String to) -> handleSend(content, to);
				case MarkAsSeenRequest(long messageId) -> handleMark(messageId);
				case AuthPollRequest(String name, String passwd) -> handlePollInit(name, passwd);
				case PollRequest() -> handlePoll();
				case LogoutRequest() -> logout();
				default -> logger.logError("Unexpected request type");
			}
		}

		/*Далее названия функций говорят сами за себя*/

		void atomicSend(Object response) throws IOException {
			if (!clientSocket.isClosed() && out != null) {
				synchronized (out){
					out.writeObject(response);
					out.flush();
				}
			}
		}

		void atomicSendToPoller(Object response) throws IOException {
			var pollerOpt = activePollers.stream().filter(
					cl -> cl.client.equals(client)
				).findFirst();
			if(pollerOpt.isPresent()) {
				pollerOpt.get().atomicSend(response);
			}
		}

		private void register(String name, String passwd) throws IOException {
			var user = registerUser(name, passwd);
			atomicSend(new AuthorisationResponse(user != null ? user.id() : -1));
			if (user != null) authoriseAsClient(user);
		}

		private void authorise(String name, String passwd) throws IOException {
			int userId = authoriseUser(name, passwd);
			atomicSend(new AuthorisationResponse(userId));
			if (userId != -1) {
				logger.logInfo("User authorised: " + name);
				authoriseAsClient(new User(userId, name));
			}
		}

		private void getNames() throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			atomicSend(
					new GetNamesResponse(
							usersById.values().stream().filter(
									user ->
											user != null && !user.equals(client)
									)
									.toList()
					)
			);
			logger.logInfo("User " + client + " got active names");
		}

		private void handleDialog(String with) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			List<Message> dialog = getDialog(client, with);
			atomicSend(new GetMessagesResponse(dialog));
		}

		private void handleGetMessages(User from, MessageSeenState state) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			atomicSend(new GetMessagesResponse(getMessages(from, client, state)));
		}

		private void handleGetAllMessages(MessageSeenState state) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			atomicSend(new GetMessagesResponse(getMessages(client, state)));
		}

		private void handleSend(MessageContent content, String to) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			if (to != null && !usersByName.containsKey(to)) {
				sendError("Recipient not found: " + to);
				return;
			}

			Integer mId = sendMessage(content, client.name(), to, LocalDateTime.now());
			if (mId != null)
				atomicSendToPoller(new SendMessageResponse(mId));
			else
				sendError("Message hasn't been sent: internal error");
		}

		private void handlePoll() throws IOException {
			if (client == null || !messageQueues.containsKey(client.name())) {
				sendError("Invalid poll request for user: " + client);
				return;
			}
			if(!authorised){
				sendError("Poller not authorised");
				return;
			}
			try {
				Message msg = messageQueues.get(client.name()).poll(30, TimeUnit.SECONDS);
				atomicSend(new GetMessagesResponse(msg != null ? List.of(msg) : List.of()));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				sendError("Poll interrupted");
			}
		}

		private void handlePollInit(String name, String passwd) throws IOException {
			int userId = authoriseUser(name, passwd);
			atomicSend(new AuthPollResponse(userId != -1));
			if (userId != -1) {
				logger.logInfo("Poller authorised: " + name);
				authoriseAsPoller(new User(userId, name));
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
					atomicSend(new InfoMessage("Message marked as seen"));
					var chatter = activePollers.stream().filter(cl -> cl.client.name().equals(updated.from())).findAny();
					chatter.ifPresent(clientHandler ->
							clientHandler.sendMarkResponse(new MarkAsSeenResponse(updated.messageId(), updated.seenTime())));
				}
			} else {
				atomicSend(new GetMessagesResponse(List.of()));
			}
		}

		private void logout(){
			try { if (in != null) in.close(); } catch (IOException ignored) {}
			try { if (out != null) out.close(); } catch (IOException ignored) {}
			try { if (!clientSocket.isClosed()) clientSocket.close(); } catch (IOException ignored) {}
			if (client != null) {
				authorised = false;
				removeQueueForUser(client);
				activeClients.remove(this);
				activePollers.remove(this);
				logger.logInfo("User disconnected: " + client.name());
			}
		}

		public void sendError(String err) throws IOException {
			logger.logError("Err message sent to " + client + " " + err);
			atomicSend(new ErrorMessage(err));
		}

		public void sendInfo(String info) throws IOException {
			logger.logInfo("Info message sent to " + client + " " + info);
			atomicSend(new InfoMessage(info));
		}

		public User getClient() {
			return this.client;
		}

		private void authoriseAsClient(User user) {
			if (user != null) {
				client = user;
				authorised = true;

				activeClients.add(this);
			}
		}

		private void authoriseAsPoller(User user) {
			if (user != null) {
				client = user;
				authorised = true;

				addQueueForUser(client);
				activePollers.add(this);
			}
		}

		public void sendMarkResponse(MarkAsSeenResponse markAsSeenResponse){
			if(!clientSocket.isClosed() && out != null){
				try {
					logger.logInfo("Mark as seen response send: " + markAsSeenResponse);
					atomicSendToPoller(markAsSeenResponse);
				} catch (Exception e) {
					logger.logError("Couldn't send mark response to " + client, e.getMessage());
				}
			}
		}
	}

	public Server(){
		running = false;
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (running) stop();
		}));
	}
	/**Поток - acceptor: принимает пользователей*/
	private Thread main;

	/**Запуск сервера на порту port*/
	public boolean start(int port){
		if(running || corrupted)
			return false;
		try{
			dbConnect("jdbc:postgresql://localhost:5432/proglab4", "postgres", "password");
			registerServer(port);
			serverSocket = new ServerSocket(port);


			running = true;
			logger.logInfo("Server started on port: " + port);
			main = new Thread(() -> {
				while (running && !serverSocket.isClosed()) {
					try {
						var ch = new ClientHandler(serverSocket.accept());
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



	/**Запуск на порту 43500*/
	public boolean start(){
		return start(BASIC_PORT);
	}

	/**Подключение к БД
	 * По сути динамические - можно задавать параметры
	 * По факту в конструкторе применяется только localhost:5432*/
	public void dbConnect(String url, String user, String password){
		corrupted = false;
		try {
			connection = DAO_Conf.getConnection();
			logger.logInfo("DB connected");
		} catch (SQLException e) {
			logger.logError("Couldn't connect to DB " + url + ", as user " + user + ".\nUnexpected error occurred", e.getMessage());
			corrupted = true;
		}
	}

	/**Остановка сервера
	 * Автоматическое отключение всех пользователей*/
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

		for(var poller : activePollers){
			poller.logout();
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
		activePollers.clear();
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	/**Регистрация/авторизация сервера
	 * Автоматическая подгрузка пользователей с this.serverId = user.serverId*/
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
					DAO_Conf.serverId = serverId;
					try(PreparedStatement stmt1 = connection.prepareStatement(
							"select username, userId from users where serverId = ?")
					){
						stmt1.setInt(1, serverId);
						try(ResultSet rs1 = stmt1.executeQuery()){
							while(rs1.next()){
								var name = rs1.getString("username");
								int userId = rs1.getInt("userId");

								var user = new User(userId, name);

								usersByName.put(name, user);
								usersById.put(userId, user);
								logger.logInfo("User fetched " + user.name());
							}
						}
					}
				} else {
					throw new SQLException("Failed to get serverId");
				}
			}
		}
	}

	////Получение локального IP
	private String getLocalIpAddress() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return "127.0.0.1";
		}
	}

	//Далее названия методов говорят сами за себя

	private User registerUser(String name, String passwd){
		try{
			var user = UserDAO.insert(name, passwd);
			if(user != null){
				usersByName.put(user.name(), user);
				usersById.put(user.id(), user);
				logger.logInfo("New user registered " + user.name());
			}
			return user;
		}catch (SQLException e) {
			logger.logError("Couldn't register user " + name + ".\nUnexpected error occurred", e.getMessage());
		}
		return null;


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
					var usr = new User(userId, name);
					usersByName.put(name, usr);
					usersById.put(userId, usr);
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

		return new Message(id, content, fromName, toName, dispatch, seen);
	}

	private List<Message> getMessages(User to, MessageSeenState state){
		return getMessages(null, to, state);
	}

	private List<Message> getMessages(User from, User to, MessageSeenState state){
		List<Message> resultList = new ArrayList<>();
		String sql = getSqlMessages(state, from);
		try (PreparedStatement stmt = connection.prepareStatement(sql)){
			Integer toId = usersByName.get(to.name()).id();
			if(toId != null)
				stmt.setInt(1, toId);
			else{
				logger.logError("Couldn't get authorised user " + to.name() + ".");
				return resultList;
			}
			if(from != null){
				Integer fromId = usersByName.get(from.name()).id();
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
			logger.logInfo("Messages get from " + (from == null ? "(all) " : from.name()) + " to " + to.name());
		} catch (SQLException e) {
			logger.logError("Couldn't get messages sent to user " + to.name() + ".\nUnexpected error occurred", e.getMessage());
		}
		return resultList;
	}

	public void addQueueForUser(User user) {
		messageQueues.putIfAbsent(user.name(), new LinkedBlockingQueue<>());
	}

	public void removeQueueForUser(User user) {
		messageQueues.remove(user.name());
	}

	public void deliverMessage(Message msg) {
		var recipient = msg.to();
		if (recipient == null) {
			for (ClientHandler ch : activeClients) {
				User u = ch.getClient();
				if (u != null && !u.name().equals(msg.from())) {
					BlockingQueue<Message> q = messageQueues.get(u.name());
					if (q != null) {
						if(!q.offer(msg))
							logger.logError("Couldn't offer a message");
						else
							logger.logInfo("Messages get with id " + msg.messageId() +
									"from" + (msg.from() == null ? "(all) " : msg.from()) +
									" to " + (msg.to() == null ? "(all) " : msg.from()));
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

	private Integer sendMessage(MessageContent content, String from, String to, LocalDateTime dispatchTime) {
		String sql = "INSERT INTO messages (content, fromUser, toUser, dispatchTime) VALUES (?, ?, ?, ?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			Integer fromId = usersByName.get(from).id();
			Integer toId = (to == null) ? null : usersByName.get(to).id();

			if (toId == null && to != null) {
				logger.logError("Recipient not found: " + to);
				return null;
			}
			if (fromId == null) {
				logger.logError("Sender not found: " + from);
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
			logger.logInfo("Message from " + from +
					" to " + (to == null ? "(broadcast)" : to) + " has been sent");
			if (affected == 1) {
				try(ResultSet rs = stmt.getGeneratedKeys()){
					if (rs.next()) {
						int messageId = rs.getInt(1);
						deliverMessage(new Message(messageId, content, from, to, dispatchTime));
						logger.logInfo("Message from " + from +
								" to " + (to == null ? "(broadcast)" : to) +
								" with " + messageId + " has been delivered");
						return messageId;
					}
				}
			}
		} catch (SQLException e) {
			logger.logError("Couldn't send message from " + from
					+ " to " + (to == null ? "(broadcast)" : to)
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
					User from = usersById.get(rs.getInt("fromUser"));
					User to = usersById.get(rs.getInt("toUser"));
					LocalDateTime dispatchTime = rs.getTimestamp("dispatchTime").toLocalDateTime();
					LocalDateTime seenTime = rs.getTimestamp("seenTime").toLocalDateTime();
					logger.logInfo("Message marked with id " + messageId);
					return new Message(messageId, new MessageContent(content), from.name(), to.name(), dispatchTime, seenTime);
				} else {
					logger.logError("Message with id: " + messageId + " not found");
				}
			}
		}catch(SQLException e){
			logger.logError("Couldn't mark message " + messageId + " as seen", e.getMessage());
		}
		return null;
	}

	private List<Message> getDialog(User me, String with) {
		List<Message> result = new ArrayList<>();
		String sql = "SELECT m.messageId, m.content, fromU.username AS fromName, toU.username AS toName, " +
				"m.dispatchTime, m.seenTime " +
				"FROM messages m " +
				"INNER JOIN users fromU ON m.fromUser = fromU.userId " +
				"LEFT JOIN users toU ON m.toUser = toU.userId " +
				"WHERE fromU.serverId = ? and toU.serverId = ? and " +
				"((fromU.username = ? AND (toU.username = ? OR m.toUser is null)) " +
				"   OR (fromU.username = ? AND (toU.username = ? OR m.toUser is null))) " +
				"ORDER BY m.dispatchTime";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, DAO_Conf.serverId);
			stmt.setInt(2, DAO_Conf.serverId);
			stmt.setString(3, me.name());
			stmt.setString(4, with);
			stmt.setString(5, with);
			stmt.setString(6, me.name());
			try(ResultSet rs = stmt.executeQuery()){
				while (rs.next()) {
					result.add(resultSetToMessage(rs));
				}
			}
			logger.logInfo("Get dialog between " + me.name() + " and " + with);
		} catch (SQLException e) {
			logger.logError("Couldn't get dialog between " + me.name() + " and " + with, e.getMessage());
		}
		return result;
	}

	/**Точка входа
	 * поддерживаются команды для консольного вызова
	 * java -jar server.jar [ФЛАГ]
	 * Флаги:
	 * -p {число} порт, на котором запустится сервер (-s не обязателен)
	 * -s флаг для запуска сервера
	 * Вызов меню управления сервером*/
	static void main(String[] args){
		Server server = new Server();
		Scanner scanner = new Scanner(System.in);
		int choice;

		int port = 43500;


		boolean autoStart = false;

		for(int i = 0; i < args.length; i++){
			switch (args[i]) {
				case "-p":
					if (i + 1 < args.length) try { port = Integer.parseInt(args[++i]); autoStart = true;} catch (NumberFormatException ignored) {}
					break;
				case "-s":
					autoStart = true;

					break;
			}
		}

		if(autoStart){
			server.start(port);
		}

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
					port = scanner.nextInt();
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