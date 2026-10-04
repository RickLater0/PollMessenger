import common.Logger;
import common.dto.Message;
import common.dto.MessageContent;
import common.dto.User;
import common.messages.ErrorMessage;
import common.messages.InfoMessage;
import common.requests.*;
import common.responses.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public final class Client implements AutoCloseable {
	private final Socket socket;
	private final ObjectInputStream in;
	private final ObjectOutputStream out;

	private final Socket pollSocket;
	private final ObjectInputStream pollIn;
	private final ObjectOutputStream pollOut;

	private final Logger logger = new Logger();

	private final Thread listenThread;

	/// /пользователь - он/офлайн
	private final Map<String, Boolean> users = new ConcurrentHashMap<>();

	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

	private volatile boolean running  = true;
	private volatile User currentUser = null;
	private volatile String chatPartner = null;

	private final BlockingQueue<Object> incomingResponses = new LinkedBlockingQueue<>();

	private final List<Message> currentDialogue = new CopyOnWriteArrayList<>();

	public Client(String address, int port) throws IOException {

		socket = new Socket(address, port);
		pollSocket = new Socket(address, port);
		out = new ObjectOutputStream(socket.getOutputStream());
		out.flush();
		in = new ObjectInputStream(socket.getInputStream());

		pollOut = new ObjectOutputStream(pollSocket.getOutputStream());
		pollOut.flush();
		pollIn = new ObjectInputStream(pollSocket.getInputStream());

		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
		listenThread = new Thread(this::listening);
		listenThread.setDaemon(true);
		listenThread.start();
	}

	void listening() {
		try {
			while (running && !socket.isClosed()) {
				Object response = in.readObject();
				if(response instanceof InfoMessage(String msg))
					logger.logInfo("Server - " + msg);
				else if(response instanceof ErrorMessage(String msg))
					logger.logError("Server ERROR - " + msg);
				else {
					if (!incomingResponses.offer(response))
						logger.logError("Couldn't offer");
				}
			}
		} catch (EOFException e) {
			//ignore
		} catch (IOException | ClassNotFoundException e) {
			if (running) System.err.println("Connection error: " + e.getMessage());
		} finally {
			close();
		}
	}

	void logout() {
		try {
			sendAsync(new LogoutRequest());
			currentUser = null;
			chatPartner = null;
			if (poller != null) {
				poller.interrupt();
			}
			System.out.println("Logged out.");
		} catch (IOException e) {
			logger.logError("Error:", e.getMessage());
		}
	}

	@Override
	public void close() {
		if (!running)
			return;
		running = false;
		try {
			if (socket != null && !socket.isClosed()) {
				try {
					sendAsync(new LogoutRequest());
				} catch (IOException ignored) {
				}
			}

			if (poller != null) {
				poller.interrupt();
			}

			// Сначала закрываем сокеты — это мгновенно разблокирует все висящие readObject()
			if (socket != null) try {
				socket.close();
			} catch (IOException ignored) {
			}
			if (pollSocket != null) try {
				pollSocket.close();
			} catch (IOException ignored) {
			}

			// Затем закрываем сами потоки данных
			if (in != null) try {
				in.close();
			} catch (IOException ignored) {
			}
			if (out != null) try {
				out.close();
			} catch (IOException ignored) {
			}
			if (pollIn != null) try {
				pollIn.close();
			} catch (IOException ignored) {
			}
			if (pollOut != null) try {
				pollOut.close();
			} catch (IOException ignored) {
			}

			users.clear();
			currentDialogue.clear();
			if (listenThread != null) listenThread.interrupt();

			clearConsole();
			System.out.println("Client resource cleanup completed.");
			System.out.println(logger.showLogs());
		} catch (Exception ignored) {
		}
	}

	private Object sendAndWait(Object request) throws IOException, InterruptedException {
		synchronized (out) {
			out.writeObject(request);
			out.flush();
		}
		var resp = incomingResponses.take();
		logger.logInfo("Sync response - " + resp.getClass().getSimpleName());
		return resp;
	}

	private void sendAsync(Object request) throws IOException {
		synchronized (out) {
			out.writeObject(request);
			out.flush();
		}
	}

	private boolean register(String name, String password) throws IOException, InterruptedException {
		Object resp = sendAndWait(new RegistrationRequest(name, password));
		if (resp instanceof AuthorisationResponse(Long userId)) {
			var success = userId != -1;
			if(success){
				login(name, password);
			}
			return success;
		}
		return false;
	}

	/**
	 * Запустить отдельный поток для long polling.
	 * Этот поток будет постоянно отправлять PollRequest и получать новые сообщения.
	 * Новые сообщения обрабатываются здесь же и, если они от текущего chatPartner,
	 * автоматически отмечаются как прочитанные и выводятся.
	 */
	private class Poller extends Thread {

		private void atomicPoll(Object request) throws IOException {
			synchronized (pollOut) {
				pollOut.writeObject(request);
				pollOut.flush();
			}
		}

		private boolean authPoll(String name, String password) throws IOException {
			atomicPoll(new AuthPollRequest(name, password));
			Object resp = null;
			try {
				resp = pollIn.readObject();
			} catch (ClassNotFoundException e) {
				logger.logError("Couldn't read response from server: " + e.getMessage());
			}
			if (resp instanceof AuthPollResponse(boolean success) && success) {
				return true;
			}
			System.err.println("Poll auth failed");
			return false;
		}

		@Override
		public void run() {
			try {
				while (running && !pollSocket.isClosed()) {
					if (currentUser == null)
						break;
					atomicPoll(new PollRequest());
					Object response = pollIn.readObject();
					handlePollResponse(response);
				}
			} catch (Exception e) {
				if (running) logger.logError("Poll error: " + e.getMessage());
			}
		}

		private void handlePollResponse(Object response) {
			switch (response) {
				case GetMessagesResponse(List<Message> messages) -> handleGetMessages(messages);
				case SendMessageResponse(long realId) -> handleSendMessage(realId);
				case MarkAsSeenResponse(long msgId, LocalDateTime seenTime) -> handleMarkAsSeen(msgId, seenTime);
				case ClientLogoutResponse(String username) -> handleOtherClientLogout(username);
				case GetActiveUsersResponse(List<String> usernames) -> handleGetActiveUsers(usernames);
				case InfoMessage(String msg) -> logger.logInfo("Server Poll: " + msg);
				case ErrorMessage(String msg) -> logger.logError("Server Poll Error: " + msg);
				default -> logger.logError("Unexpected poll response: " + response);
			}
		}

		private void handleGetMessages(List<Message> messages) {
			for (Message msg : messages) {
				if (msg == null) {
					logger.logError("Incoming message is null!");
					continue;
				}
				if (chatPartner != null && chatPartner.equals(msg.from())) {
					currentDialogue.add(msg);
					try {
						sendAsync(new MarkAsSeenRequest(msg.messageId()));
					} catch (IOException ignored) {
					}
					msg.setSeenTime(LocalDateTime.now());
					redrawChatScreen();
				} else if (chatPartner == null && (msg.to() == null || msg.to().equals(currentUser.name()))) {
					System.out.print("\n[New from " + msg.from() + "]: " + msg.content().content());
					System.out.print("\n> ");
				}
			}
		}

		private void handleSendMessage(long realId) {
			if (chatPartner != null) {
				Message toRemove = null;
				for (Message msg : currentDialogue) {
					if (msg.messageId() < 0 && msg.from().equals(currentUser.name()) && (msg.to() == null || msg.to().equals(chatPartner))) {
						toRemove = msg;
						break;
					}
				}
				if (toRemove != null) {
					currentDialogue.remove(toRemove);
					currentDialogue.add(new Message(realId, toRemove.content(), toRemove.from(), toRemove.to(),
							toRemove.dispatchTime(), toRemove.seenTime()));
					redrawChatScreen();
				}
			}
		}

		private void handleMarkAsSeen(long msgId, LocalDateTime seenTime) {
			for (Message msg : currentDialogue) {
				if (msg.from().equals(currentUser.name()) && msg.messageId() == msgId) {
					msg.setSeenTime(seenTime);
					redrawChatScreen();
				}
			}
		}

		private void handleOtherClientLogout(String username) {
			users.put(username, false);
		}

		private void handleGetActiveUsers(List<String> usernames) {
			for (String username : usernames) {
				users.put(username, true);
			}
		}
	}

	private volatile Poller poller = null;

	/**
	 * Авторизация существующего пользователя.
	 * При успехе сохраняет currentUser и запрашивает список активных пользователей.
	 */
	@SuppressWarnings(value = "BooleanMethodIsAlwaysInverted")
	private boolean login(String name, String password) throws IOException, InterruptedException {
		Object resp = sendAndWait(new AuthorisationRequest(name, password));
		if (resp instanceof AuthorisationResponse(Long userId) && userId != -1) {

			poller = new Poller();

			if (poller.authPoll(name, password)) {
				currentUser = new User(userId, name);
				System.out.println("Login successful as " + name);
				System.out.println("Welcome to Messenger!");
				System.out.println("Print /help to see commands");
				showUsers();
				poller.setDaemon(true);
				poller.start();
				return true;
			}
		}
		System.err.println("Login failed");
		return false;
	}

	/**
	 * Получить и вывести список активных пользователей (кроме себя).
	 */
	private void showUsers() throws IOException, InterruptedException {
		if (users.isEmpty()) {
			Object resp = sendAndWait(new GetNamesRequest());
			if (resp instanceof GetNamesResponse(Map<String, Boolean> usersGot)) {
				synchronized (users) {
					users.putAll(usersGot);
				}
			} else if (resp instanceof ErrorMessage(String message)) {
				logger.logError(message);
			}
		}
		if (users.isEmpty()) {
			System.out.println("No other users.");
		} else {
			System.out.println("Users:");
			for (var entry : users.entrySet()) {
				System.out.println(entry.getKey() + "\t" + (entry.getValue() ? "[Y]" : "[N]"));
			}
		}
	}

	/**
	 * Открыть чат с выбранным пользователем.
	 * - Загружает диалог (все сообщения между пользователями).
	 * - Отмечает все непрочитанные сообщения от этого пользователя как прочитанные.
	 * - Переходит в режим чата (chatPartner = with).
	 */
	public void openChat(String with) throws IOException, InterruptedException {
		if (with.equals(currentUser.name())) {
			logger.logInfo("You cannot chat with yourself.");
			return;
		}
		Object resp = sendAndWait(new GetDialogRequest(with));
		if (resp instanceof GetMessagesResponse(List<Message> dialog)) {
			if (dialog == null) {
				logger.logInfo("No messages found. Probably a typo in username");
				System.out.println(logger.getLastLog());
				return;
			}
			currentDialogue.clear();
			currentDialogue.addAll(dialog);
			chatPartner = with;

			for (Message msg : currentDialogue) {
				if (msg.to() != null && msg.to().equals(currentUser.name()) && msg.from().equals(with) && msg.seenTime() == null) {
					sendAsync(new MarkAsSeenRequest(msg.messageId()));
					msg.setSeenTime(LocalDateTime.now());
				}
			}
			redrawChatScreen();
		} else if (resp instanceof ErrorMessage(String message)) {
			logger.logError(message);
		}
	}

	/**
	 * Отправить сообщение текущему собеседнику в чате.
	 */
	public void sendMessageToChat(String text) throws IOException {
		if (chatPartner == null) return;
		long tempId = -System.currentTimeMillis();
		Message tempMsg = new Message(tempId, new MessageContent(text), currentUser.name(), chatPartner, LocalDateTime.now());
		currentDialogue.add(tempMsg);
		redrawChatScreen();
		sendAsync(new SendMessageRequest(new MessageContent(text), chatPartner));
	}

	/**
	 * Отправить broadcast-сообщение всем пользователям.
	 */
	public void broadcast(String text) throws IOException {
		sendAsync(new SendMessageRequest(new MessageContent(text), null));
		System.out.println("[Broadcast] " + text);
	}

	/**
	 * Выйти из текущего чата.
	 */
	private void exitChat() {
		chatPartner = null;
		currentDialogue.clear();
		clearConsole();
		System.out.println("You left the chat.");
	}

	private void redrawChatScreen() {
		if (chatPartner == null) return;
		clearConsole();
		System.out.println("=== Chat with " + chatPartner+ " ===");
		if (currentDialogue.isEmpty()) {
			System.out.println("No messages yet.");
		} else {
			for (Message msg : currentDialogue) {
				String sender = msg.from().equals(currentUser.name()) ? "You" : msg.from();
				String time = msg.dispatchTime().format(TIME_FORMATTER);

				if (msg.seenTime() != null) {
					System.out.printf("[%s>%s] %s: %s%n", time, msg.seenTime().format(TIME_FORMATTER), sender, msg.content().content());
				} else {
					if (msg.messageId() < 0)
						System.out.printf("[%s>... ] %s: %s%n", time, sender, msg.content().content());
					else
						System.out.printf("[%s>sent] %s: %s%n", time, sender, msg.content().content());
				}
			}
		}
		System.out.print("> ");
	}


	private String getHelp(){
		return """
				Commands:\s
				  /users          - show active users
				  /chat <name>    - open chat with user
				  /broadcast <msg> - send message to everyone
				  /logout         - logout
				  /exit           - quit client
				  /logs show      - show logs
				  /logs clear     - purge logs list
				  /clear          - clear console window
				  /help           - see this list
				""";
	}

	//TODO переписать консольный UI

	private void clearConsole() {
		try {
			String os = System.getProperty("os.name").toLowerCase();
			if (os.contains("win")) {
				new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
			} else {
				System.out.print("\033[H\033[2J");
				System.out.flush();
			}
		} catch (Exception e) {
			// ignore
		}
	}

	private void console() {
		Scanner scanner = new Scanner(System.in);
		boolean exit = false;

		while (!exit && running) {
			if (currentUser == null) {
				System.out.println("\n1. Login\n2. Register\n0. Exit");
				System.out.print("Choice: ");
				String line = scanner.nextLine().trim();
				switch (line) {
					case "1" -> {
						System.out.print("Username: ");
						String name = scanner.nextLine().trim();
						System.out.print("Password: ");
						String pass = scanner.nextLine().trim();
						try {
							if (!login(name, pass))
								System.err.println("Invalid username or password");
						} catch (IOException | InterruptedException e) {
							logger.logError("Error:", e.getMessage());
						}
					}
					case "2" -> {
						System.out.print("Username: ");
						String name = scanner.nextLine().trim();
						System.out.print("Password: ");
						String pass = scanner.nextLine().trim();
						try {
							if (register(name, pass)) {
								System.out.println("Registration successful");
							} else {
								System.out.println("Registration failed (username may exist).");
							}
						} catch (Exception e) {
							System.err.println("Registration error: " + e.getMessage());
						}
					}
					case "0" -> exit = true;
					default -> System.out.println("Invalid choice");
				}
			} else {
				String input;
				if (chatPartner == null) {
					System.out.print("> ");
					input = scanner.nextLine().trim();
					if (input.startsWith("/users")) {
						try {
							showUsers();
						} catch (Exception e) {
							logger.logError("Error:", e.getMessage());
						}
					} else if (input.startsWith("/chat ")) {
						String target = input.substring(6).trim();
						if (!target.isEmpty()) {
							try {
								openChat(target);
							} catch (Exception e) {
								logger.logError("Error:", e.getMessage());
							}
						} else {
							System.out.println("Usage: /chat username");
						}
					} else if (input.startsWith("/broadcast ")) {
						String msg = input.substring(11).trim();
						if (!msg.isEmpty()) {
							try {
								broadcast(msg);
							} catch (IOException e) {
								logger.logError("Error:", e.getMessage());
							}
						}
					} else if (input.equals("/logs show")) {
						System.out.println("-=LOGS=-");
						System.out.println(logger.showLogs());
					} else if (input.equals("/logs clear")) {
						System.out.println("Cleared");
						logger.clearLogs();
					} else if (input.equals("/clear")) {
						clearConsole();
					} else if (input.equals("/help")) {
						System.out.println(getHelp());
					} else if (input.equals("/logout")) {
						logout();
					} else if (input.equals("/exit")) {
						exit = true;
					} else {
						System.out.println("Unknown command. Type /users, /chat, /broadcast, /logout, /exit");
					}
				} else {
					System.out.print("[c:" + chatPartner + "] > ");
					input = scanner.nextLine().trim();
					if (input.equals("/exit")) {
						exitChat();
					} else if (input.equals("/help")) {
						System.out.println(getHelp());
					} else if (!input.isEmpty()) {
						try {
							sendMessageToChat(input);
						} catch (IOException e) {
							System.err.println("Send failed: " + e.getMessage());
						}
					}
				}
			}
		}
		close();
		scanner.close();
	}

	/**
	 * Точка входа
	 * поддерживаются команды для консольного вызова
	 * java -jar client.jar [ФЛАГИ]
	 * Флаги:
	 * -i {ip:port} адрес сервера
	 * -r логин для РЕГИСТРАЦИИ
	 * -l логин для АВТОРИЗАЦИИ (-r и -l не могут существовать вместе)
	 * -p пароль для входа
	 * Далее - терминал управления клиентом
	 * /help для помощи
	 */
	static void main(String[] args) {
		// Параметры по умолчанию
		String host = "127.0.0.1";
		int port = 43500;
		boolean isIpSet = false;
		String loginName = null;
		String registerName = null;
		String loginPassword = null;
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-i":
					if (i + 1 < args.length) {
						String serverArg = args[++i];
						String[] parts = serverArg.split(":");
						host = parts[0];
						if (parts.length > 1) {
							try {
								port = Integer.parseInt(parts[1]);
							} catch (NumberFormatException ignored) {
							}
						}
						isIpSet = true;
					}
					break;
				case "-r":
					if (i + 1 < args.length) registerName = args[++i];
					break;
				case "-l":
					if (i + 1 < args.length) loginName = args[++i];
					break;
				case "-p":
					if (i + 1 < args.length) loginPassword = args[++i];
					break;
			}
		}

		// Запрос адреса сервера, если не задан через флаги
		if (!isIpSet) {
			Scanner scanner = new Scanner(System.in);
			System.out.print("Server address (ip:port) [127.0.0.1:43500]: ");
			String input = scanner.nextLine().trim();
			if (!input.isEmpty()) {
				String[] parts = input.split(":");
				host = parts[0];
				if (parts.length > 1) {
					try {
						port = Integer.parseInt(parts[1]);
					} catch (NumberFormatException ignored) {
					}
				}
			}
		}

		try (Client client = new Client(host, port)){
			// Каша из логики флагов и выводов ошибок подключения
			if (loginName != null && registerName == null && loginPassword != null) {
				if (!client.login(loginName, loginPassword)) {
					System.err.println("Auto-login failed. Starting manual mode.");
				}
			} else if (loginName == null && registerName != null && loginPassword != null) {
				if (!client.register(registerName, loginPassword)) {
					System.err.println("Auto-register failed. Starting manual mode.");
				}
			} else if (loginName == null && registerName == null && loginPassword != null) {
				System.err.println("Can't register only with password.");
			} else if (loginName != null && registerName != null) {
				System.err.println("-l and -r can't use both.");
			} else if (loginName != null || registerName != null) {
				System.err.println("Can't login without password");
			}
			client.console();
		} catch (IOException | InterruptedException e) {
			System.err.println("Cannot connect to server: " + e.getMessage());
		}
	}
}