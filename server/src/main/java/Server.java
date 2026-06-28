import DAO.DAO_Conf;
import DAO.MessageDAO;
import DAO.ServerDAO;
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

	private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

	/**Основной инструмент, записывающий логи*/
	private final Logger logger = new Logger();

	public String showLogs(){
		return logger.showLogs();
	}

	public void clearLogs(){
		logger.clearLogs();
	}

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
	private class ClientHandler implements Runnable {
		private final Socket clientSocket;
		private User client = null;
		private boolean authorised = false;

		private final ObjectOutputStream out;
		private final ObjectInputStream in;

		public ClientHandler(Socket socket) throws IOException {
			this.clientSocket = socket;
			out = new ObjectOutputStream(clientSocket.getOutputStream());
			in = new ObjectInputStream(clientSocket.getInputStream());
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
			try{
				switch (request) {
					case RegistrationRequest(String name, String passwd) -> register(name, passwd);
					case AuthorisationRequest(String name, String passwd) -> authorise(name, passwd);
					case GetNamesRequest() -> getNames();
					case GetDialogRequest(String with) -> handleDialog(with);
					case GetMessagesRequest(User from, MessageSeenState state) -> handleGetMessages(from, state);
					case SendMessageRequest(MessageContent content, String to) -> handleSend(content, to);
					case MarkAsSeenRequest(long messageId) -> handleMark(messageId);
					case AuthPollRequest(String name, String passwd) -> handlePollInit(name, passwd);
					case PollRequest() -> handlePoll();
					case LogoutRequest() -> logout();
					default -> logger.logError("Unexpected request type");
				}
			} catch (Exception e) {
				logger.logError("Unexpected error in client handler: ", e.getMessage());
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
			logger.logInfo("Authorised: " + (userId != -1 ? userId : "unknown"));
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
			logger.logInfo(client + " got active names");
		}

		private void handleDialog(String with) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			if(!usersByName.containsKey(with)) {
				sendError("Error: User does not exist: " + with);
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

			List<Message> messages = getMessages(from, client, state);
			if(messages == null) {
				sendError("Error: No messages found");
				logger.logError("Couldn't get messages " + client,
						"Someones id is null");
				return;
			}
			atomicSend(new GetMessagesResponse(messages));
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

			List<Long> msId = sendMessage(content, client, to, LocalDateTime.now());
			if(msId == null) {
				sendError("Could not send message: " + to + ": Server error");
				return;
			}
			for(var mId : msId){
				if (mId != -1)
					atomicSendToPoller(new SendMessageResponse(mId));
				else
					sendError("Message hasn't been sent: internal error");
			}
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
					var chatter = activePollers.stream().filter(
							cl -> cl.client.name().equals(updated.from())
					).findAny();

					chatter.ifPresent(clientHandler ->
							clientHandler.sendMarkResponse(
									new MarkAsSeenResponse(updated.messageId(), updated.seenTime())
							)
					);
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

	/**Запуск сервера на порту port*/
	public boolean start(int port){
		if(running || corrupted)
			return false;
		try{
			dbConnect();
			registerServer(port);
			serverSocket = new ServerSocket(port);

			running = true;
			logger.logInfo("Server started on port: " + port);
			virtualThreadExecutor.submit(() -> {
				while (running && !serverSocket.isClosed()) {
					try {
						var ch = new ClientHandler(serverSocket.accept());
						virtualThreadExecutor.submit(ch);
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

	/**Подключение к БД*/
	public void dbConnect(){
		corrupted = false;
		try {
			DAO_Conf.getConnection();
			logger.logInfo("DB connected");
		} catch (SQLException e) {
			logger.logError("Couldn't connect to DB" + ".\nUnexpected error occurred", e.getMessage());
			corrupted = true;
		}
	}

	/**Остановка сервера
	 * Автоматическое отключение всех пользователей*/
	public boolean stop(){
		if(!running || corrupted)
			return false;

		running = false;

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

		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.logError("Couldn't stop server.\nUnexpected error occurred", e.getMessage());
			return false;
		}

		virtualThreadExecutor.shutdown();
		try {
			if (!virtualThreadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
				virtualThreadExecutor.shutdownNow();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			virtualThreadExecutor.shutdownNow();
		}

		activeClients.clear();
		activePollers.clear();
		usersByName.clear();
		usersById.clear();

		try {
			DAO_Conf.closeConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	/**Регистрация/авторизация сервера
	 * Автоматическая подгрузка пользователей с this.serverId = user.serverId*/
	private void registerServer(int port) throws SQLException {
		var users = ServerDAO.register(getLocalIpAddress(), port);
		for(var user : users){
			usersById.put(user.id(), user);
			usersByName.put(user.name(), user);
			logger.logInfo("Fetched user: " + user);
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
		try{
			var user = UserDAO.authorise(name, passwd);
			if(user != null)
				return user.id();
			return -1;

		}catch (SQLException e) {
			logger.logError("Couldn't authorise user " + name + ".\nUnexpected error occurred", e.getMessage());
		}
		return -1;
	}

	private List<Message> getMessages(User from, User to, MessageSeenState state){
		List<Message> resultList = new ArrayList<>();
		try {
			resultList = MessageDAO.getMessages(from, to, state);
			logger.logInfo("Messages get from " +  from.name() + " to " + to.name());
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

	private List<Long> sendMessage(
			MessageContent content,
			User from,
			String to,
			LocalDateTime dispatchTime
	) {
		try{
			List<Long> messagesId = new ArrayList<>();

			if(to == null){
				List<Integer> targetIds = new ArrayList<>();
				List<User> targetUsers = new ArrayList<>();

				for (User u : usersByName.values()) {
					if (!u.equals(from)) {
						targetIds.add(u.id());
						targetUsers.add(u);
					}
				}

				if (targetIds.isEmpty()) return new ArrayList<>();

				List<Long> messagesIds = MessageDAO.insertBroadcast(content, from.id(), targetIds, dispatchTime);

				for (int i = 0; i < targetUsers.size(); i++) {
					User targetUser = targetUsers.get(i);
					long msgId = messagesIds.get(i);
					if(msgId != -1)
						deliverMessage(new Message(msgId, content, from.name(), targetUser.name(), dispatchTime));
				}

				return messagesIds;
			}else {
				long id = MessageDAO.insert(
						content,
						from.id(),
						usersByName.get(to).id(),
						dispatchTime
				);
				messagesId.add(id);
				if(id != -1)
					deliverMessage(new Message(id, content, from.name(), to, dispatchTime));
			}

			return messagesId;
		}
		catch (SQLException e) {
			logger.logError("Couldn't send message from " + from
					+ " to " + (to == null ? "(broadcast)" : to)
					+ ".\nUnexpected error occurred", e.getMessage());
		}
		return null;
	}

	private Message markMessageAsSeen(long messageId){
		try {
			return MessageDAO.markMessageAsSeen(messageId);
		} catch (SQLException e) {
			logger.logError("Could not mark message as seen [id: "+ messageId +" ]", e.getMessage());
			return null;
		}
	}

	private List<Message> getDialog(User first, String second) {
		List<Message> result = new ArrayList<>();
		try {
			result = MessageDAO.getDialog(first.id(), usersByName.get(second).id());
			logger.logInfo("User " + first +"get dialog with " + second);
		} catch (SQLException e) {
			logger.logError("Couldn't get dialog between " + first + " and " + second, e.getMessage());
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

		//TODO переписать консоль сервера под команды
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