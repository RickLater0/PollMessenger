import DAO.DAO_Conf;
import DAO.MessageDAO;
import DAO.ServerDAO;
import DAO.UserDAO;
import common.Logger;
import common.dto.Message;
import common.dto.MessageContent;
import common.dto.MessageSeenState;
import common.dto.User;
import common.messages.ErrorMessage;
import common.messages.InfoMessage;
import common.requests.*;
import common.responses.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public final class Server {

	public static final int BASIC_PORT = 43500;

	private ServerSocket serverSocket;
	/**Активные клиенты*/
	private final List<ClientHandler> activeClients = new CopyOnWriteArrayList<>();
	/**Действующие poller-сокеты*/
	private final List<ClientHandler> activePollers = new CopyOnWriteArrayList<>();
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

	private boolean verbose = false;
	
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

		private final ReentrantLock sendLock = new ReentrantLock();

		public ClientHandler(Socket socket) throws IOException {
			this.clientSocket = socket;
			out = new ObjectOutputStream(clientSocket.getOutputStream());
			out.flush();
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
				logInfo("Disconnected: " + (client != null ? client : "unknown") + " due to exception: " + e.getMessage());
			} catch (IOException | ClassNotFoundException e) {
				String msg = e.getMessage();
				if (msg != null && (msg.contains("Connection reset") || msg.contains("Broken pipe"))) {
					logInfo("Disconnected (RST): " + (client != null ? client : "unknown"));
				} else {
					logError("Unexpected error in client handler: ", msg);
				}
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
					case GetActiveUsersRequest() -> getActiveNames();
					case GetDialogRequest(String with) -> handleDialog(with);
					case GetMessagesRequest(User from, MessageSeenState state) -> handleGetMessages(from, state);
					case SendMessageRequest(MessageContent content, String to) -> handleSend(content, to);
					case MarkAsSeenRequest(long messageId) -> handleMark(messageId);
					case AuthPollRequest(String name, String passwd) -> handlePollInit(name, passwd);
					case PollRequest() -> handlePoll();
					case LogoutRequest() -> logout();
					default -> logError("Unexpected request type");
				}
			} catch (Exception e) {
				logError("Unexpected error in client handler: ", e.getMessage());
			}
		}

		/*Далее названия функций говорят сами за себя*/

		void atomicSend(Object response) throws IOException {
			if (!clientSocket.isClosed() && out != null) {
				sendLock.lock(); //необязательно на java 24+, но в теории лучше будет так
				// это +масштабируемость и +вайб
				try {
					out.writeObject(response);
					out.flush();
				} finally {
					sendLock.unlock();
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
			if (user != null) {
				authoriseAsClient(user);
				logInfo("Registered: " + user);
			}
		}

		private void authorise(String name, String passwd) throws IOException {
			long userId = authoriseUser(name, passwd);
			atomicSend(new AuthorisationResponse(userId));
			if (userId != -1) {
				var user = new User(userId, name);
				authoriseAsClient(user);
				logInfo("Authorised: " + user);
			}
		}

		private void getNames() throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			try {
				atomicSend(
						new GetNamesResponse(
								getUsers(client.id())
						)
				);
			} catch (SQLException e) {
				logError("Couldn't get users", e.getMessage());
			}
			logInfo(client + " got active names");
		}

		private void getActiveNames() throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			try {
				atomicSend(
						new GetActiveUsersResponse(
								getClientsNamesExcept(client)
						)
				);
			} catch (IOException ignored) {
			}
		}

		private void handleDialog(String with) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}
			try {
				var second = UserDAO.getByName(with);
				if (second == null) {
					sendError("Error: User does not exist: " + with);
					atomicSend(new GetMessagesResponse(null));
					return;
				}
				List<Message> dialog = getDialog(client, second);
				atomicSend(new GetMessagesResponse(dialog));
			} catch (SQLException e) {
				sendError("Error: Could not get dialog with: " + with);
			}

		}

		private void handleGetMessages(User from, MessageSeenState state) throws IOException {
			if (!authorised) {
				sendError("Error: User not authorised");
				return;
			}

			List<Message> messages = getMessages(from, client, state);
			if(messages == null) {
				sendError("Error: No messages found");
				logError("Couldn't get messages " + client,
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
			try {
				var recipient = UserDAO.getByName(to);

				if (to != null && recipient == null) {
					sendError("Recipient not found: " + to);
					return;
				}

				List<Long> msId = sendMessage(content, client, recipient, LocalDateTime.now());
				if (msId == null) {
					sendError("Could not send message. Server error");
					return;
				}
				for (var mId : msId) {
					if (mId != -1)
						atomicSendToPoller(new SendMessageResponse(mId));
					else
						sendError("Message hasn't been sent: internal error");
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
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
			long userId = authoriseUser(name, passwd);
			atomicSend(new AuthPollResponse(userId != -1));
			if (userId != -1) {
				logInfo("Poller authorised: " + name);
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
						logError("SenderQueue error");
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
				if (activeClients.contains(this)) {
					activeClients.remove(this);
					activePollers.stream()
							.filter(p -> this.client.equals(p.getUser()))
							.findFirst()
							.ifPresent(p -> {
								activePollers.remove(p);
								p.logout();
							});
				} else
					activePollers.remove(this);


				try {
					sendToAllPollers(new ClientLogoutResponse(client.name()));
				} catch (IOException ignored) {
				}
				logInfo("Disconnected: " + client.name());
			}
		}

		public void sendError(String err) throws IOException {
			logError("Err message sent to " + client + " " + err);
			atomicSend(new ErrorMessage(err));
		}

		public void sendInfo(String info) throws IOException {
			logInfo("Info message sent to " + client + " " + info);
			atomicSend(new InfoMessage(info));
		}

		public User getUser() {
			return this.client;
		}

		private void authoriseAsClient(User user) {
			if (user != null) {
				client = user;
				authorised = true;

				try {
					sendToAllPollers(new GetActiveUsersResponse(List.of(user.name())));
				} catch (IOException ignored) {
				}

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
					logInfo("Mark as seen response send: " + markAsSeenResponse);
					atomicSendToPoller(markAsSeenResponse);
				} catch (Exception e) {
					logError("Couldn't send mark response to " + client, e.getMessage());
				}
			}
		}
	}

	private void sendToAllPollers(Object message) throws IOException {
		for (var poller : activePollers) {
			poller.atomicSend(message);
		}
	}

	private Map<String, Boolean> getUsers(long userId) throws SQLException {
		var res = new HashMap<String, Boolean>();
		var users = UserDAO.getAllExcept(userId);
		for (User user : users) {
			res.put(user.name(), activeClients.stream().anyMatch(cl -> cl.getUser().equals(user)));
		}
		return res;
	}

	private List<String> getClientsNamesExcept(User user) {
		List<String> names = new ArrayList<>();
		for (var client : activeClients) {
			if (!client.client.equals(user))
				names.add(client.getUser().name());
		}
		return names;
	}

	public Server(){
		running = false;
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (running) stop();
		}));
	}

	/**Запуск сервера на порту port*/
	public boolean start(int port){
		if (running) {
			logInfo("Server is already running");
			return false;
		}
		if (corrupted) {
			logError("Server is corrupted");
			return false;
		}
		try{
			dbConnect();
			registerServer(port);
			serverSocket = new ServerSocket(port);

			running = true;
			logInfo("Server started on port: " + port);
			virtualThreadExecutor.submit(() -> {
				while (running && !serverSocket.isClosed()) {
					try {
						var ch = new ClientHandler(serverSocket.accept());
						virtualThreadExecutor.submit(ch);
					} catch (SocketException e) {
						if (running) {
							logError("Socket closed unexpectedly", e.getMessage());
						}
						break;
					} catch (IOException e) {
						logError("Couldn't accept new client.\nUnexpected error occurred", e.getMessage());
					}
				}
			});
			return true;

		} catch (IOException | SQLException e){
			logError("Couldn't start server on port: " + port + ".\nUnexpected error occurred", e.getMessage());
			return false;
		}
	}

	/**Подключение к БД*/
	public void dbConnect(){
		corrupted = false;
		try {
			DAO_Conf.getConnection();
			logInfo("DB connected");
		} catch (SQLException e) {
			logError("Couldn't connect to DB" + ".\nUnexpected error occurred", e.getMessage());
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
				logError("Server closing: Error occurred when sending info message", e.getMessage());
			}
			client.logout();
		}

		for(var poller : activePollers){
			poller.logout();
		}

		try {
			serverSocket.close();
		} catch (IOException e) {
			logError("Couldn't stop server.\nUnexpected error occurred", e.getMessage());
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
			logInfo("Fetched user: " + user);
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
				logInfo("New user registered " + user.name());
			}
			return user;
		}catch (SQLException e) {
			logError("Couldn't register user " + name + ".\nUnexpected error occurred", e.getMessage());
		}
		return null;
	}

	private long authoriseUser(String name, String passwd) {
		try{
			var user = UserDAO.authorise(name, passwd);
			if(user != null)
				return user.id();
			return -1;

		}catch (SQLException e) {
			logError("Couldn't authorise user " + name + ".\nUnexpected error occurred", e.getMessage());
		}
		return -1;
	}

	private List<Message> getMessages(User from, User to, MessageSeenState state){
		List<Message> resultList = new ArrayList<>();
		try {
			resultList = MessageDAO.getMessages(from, to, state);
			logInfo("Messages get from " + from.name() + " to " + to.name());
		} catch (SQLException e) {
			logError("Couldn't get messages sent to user " + to.name() + ".\nUnexpected error occurred", e.getMessage());
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
				User u = ch.getUser();
				if (u != null && !u.name().equals(msg.from())) {
					BlockingQueue<Message> q = messageQueues.get(u.name());
					if (q != null) {
						if(!q.offer(msg))
							logError("Couldn't offer a message");
						else
							logInfo("Messages get with id " + msg.messageId() +
									"from" + (msg.from() == null ? "(all) " : msg.from()) +
									" to " + (msg.to() == null ? "(all) " : msg.from()));
					}
				}
			}
		} else {
			BlockingQueue<Message> q = messageQueues.get(recipient);
			if (q != null)
				if(!q.offer(msg))
					logError("Couldn't offer a message");
		}
	}

	private List<Long> sendMessage(
			MessageContent content,
			User from,
			User to,
			LocalDateTime dispatchTime
	) {
		try{
			List<Long> messagesId = new ArrayList<>();

			if(to == null){

				Map<User, Long> messages = MessageDAO.insertBroadcast(content, from.id(), dispatchTime);

				for (User target : messages.keySet()) {
					long msgId = messages.get(target);
					if(msgId != -1)
						deliverMessage(new Message(msgId, content, from.name(), target.name(), dispatchTime));
				}

				return new ArrayList<>(messages.values());
			}else {
				long id = MessageDAO.insert(
						content,
						from.id(),
						to.id(),
						dispatchTime
				);
				messagesId.add(id);
				if(id != -1)
					deliverMessage(new Message(id, content, from.name(), to.name(), dispatchTime));
			}

			return messagesId;
		}
		catch (SQLException e) {
			logError("Couldn't send message from " + from
					+ " to " + (to == null ? "(broadcast)" : to)
					+ ".\nUnexpected error occurred", e.getMessage());
		}
		return null;
	}

	private Message markMessageAsSeen(long messageId){
		try {
			return MessageDAO.markMessageAsSeen(messageId);
		} catch (SQLException e) {
			logError("Could not mark message as seen [id: " + messageId + " ]", e.getMessage());
			return null;
		}
	}

	private List<Message> getDialog(User first, User second) {
		List<Message> result = new ArrayList<>();
		try {
			result = MessageDAO.getDialog(first.id(), second.id());
			logInfo("User " + first + "get dialog with " + second);
		} catch (SQLException e) {
			logError("Couldn't get dialog between " + first + " and " + second, e.getMessage());
		}
		return result;
	}

	private void logError(String msg, String e) {
		logger.logError(msg, e);
		if (verbose)
			System.out.println(logger.getLastLog());
	}

	private void logError(String msg) {
		logger.logError(msg);
		if (verbose)
			System.out.println(logger.getLastLog());
	}

	private void logInfo(String msg) {
		logger.logInfo(msg);
		if (verbose)
			System.out.println(logger.getLastLog());
	}
	

	/**Точка входа
	 * поддерживаются команды для консольного вызова
	 * java -jar server.jar [ФЛАГ]
	 * Флаги:
	 * -p {число} порт, на котором запустится сервер (-s не обязателен)
	 * -s флаг для запуска сервера
	 * -v сразу выводит логи при получении
	 * Вызов меню управления сервером*/

	static void main(String[] args){
		Server server = new Server();
		Scanner scanner = new Scanner(System.in);
		String input;

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
				case "-v"://можно добавить ещё флаги, какие именно типы логов будут высвечиваться
					server.verbose = true;
					break;
			}
		}

		if(autoStart){
			server.start(port);
		}

		do {

			input = scanner.nextLine().trim();

			if (input.charAt(0) != '/') {
				System.out.println("Command must start with '/'");
			} else {
				input = input.substring(1);
				String[] commands = input.split("\\s+");
				switch (commands[0]) {
					case "start":
						port = Server.BASIC_PORT;
						if (commands.length == 2) {
							try {
								port = Integer.parseInt(commands[1]);
							} catch (NumberFormatException ignored) {
								System.out.println("Port must be an integer");
							}
						}
						if (server.start(port))
							System.out.println("Server started");
						else
							System.out.println("Could not start server");
						break;
					case "stop":
						if (server.stop())
							System.out.println("Server stopped");
						else
							System.out.println("Could not stop server");
						break;
					case "logs":
						if (commands.length == 1) {
							System.out.println("/logs show/clear");
							break;
						}
						switch (commands[1]) {
							case "clear":
								switch (commands.length) {
									case 2:
										server.clearLogs();
										server.logInfo("Logs cleared");
										System.out.println("Logs cleared");
										break;
									case 3:
										switch (commands[2]) {
											case "all":
												server.clearLogs();
												server.logInfo("Logs cleared");
												System.out.println("Logs cleared");
												break;
											case "info":
												server.logger.clearInfo();
												server.logInfo("Info logs cleared");
												System.out.println("Info logs cleared");
												break;
											case "error":
											case "err":
												server.logger.clearErrors();
												server.logInfo("Error logs cleared");
												System.out.println("Error logs cleared");
												break;
										}
								}
								break;
							case "show":
								switch (commands.length) {
									case 2:
										System.out.println("-=LOGS=-");
										System.out.println(server.showLogs());
										break;
									case 3:
										switch (commands[2]) {
											case "all":
												System.out.println("-=LOGS=-");
												System.out.println(server.showLogs());
												break;
											case "info":
												System.out.println("-=LOGS=-");
												System.out.println(server.logger.showInfoLogs());
												break;
											case "error":
											case "err":
												System.out.println("-=LOGS=-");
												System.out.println(server.logger.showErrorLogs());
												break;
										}
								}
								break;
						}
						break;
					case "exit":
						server.stop();
						scanner.close();
						return;
					case "help":
						System.out.println("""
								/help - see this list
								/start [port] - start server on port. Basic port is 43500
								/stop - stop the server
								/logs {show/clear} [all/info/err]
								""");
						break;
					case "clear":
						try {
							String os = System.getProperty("os.name").toLowerCase();
							if (os.contains("win")) {
								new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
							} else {
								System.out.print("\033[H\033[2J");
								System.out.flush();
							}
						} catch (Exception ignored) {
						}
						break;
					default:
						System.out.println("Invalid command. Try /help");
						break;
				}
			}
			System.out.print("\n");
		} while (true);
	}
}