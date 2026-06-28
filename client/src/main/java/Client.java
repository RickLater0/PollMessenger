import common.Logger;
import common.dto.*;
import common.requests.*;
import common.responses.*;
import common.messages.*;

import java.net.*;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public final class Client {
	private final Socket socket;
	private final ObjectInputStream in;
	private final ObjectOutputStream out;

	private final Socket pollSocket;
	private final ObjectInputStream pollIn;
	private final ObjectOutputStream pollOut;

	private final Logger logger = new Logger();

	private final Thread listenThread;

	//TODO создать коллекцию для хранения кешированных пользователей. Использовать её при открытии чата и других взаимодействиях
	// с другими пользователями

	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

	private volatile boolean running  = true;
	private volatile User currentUser = null;
	private volatile String chatPartner = null;

	private final BlockingQueue<Object> incomingResponses = new LinkedBlockingQueue<>();

	List<Message> currentDialogue = new CopyOnWriteArrayList<>();

	public Client(String address, int port) throws IOException {

		socket = new Socket(address, port);
		pollSocket = new Socket(address, port);
		in = new ObjectInputStream(socket.getInputStream());
		out = new ObjectOutputStream(socket.getOutputStream());

		pollIn = new ObjectInputStream(pollSocket.getInputStream());
		pollOut = new ObjectOutputStream(pollSocket.getOutputStream());

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
					logger.logInfo("Server ERROR - " + msg);
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
			poller.logout();
			currentUser = null;
			chatPartner = null;
			System.out.println("Logged out.");
		} catch (IOException e) {
			logger.logError("Error:", e.getMessage());
		}
	}

	public void close() {
		running = false;
		try {
			if (socket != null && !socket.isClosed()) {
				sendAsync(new LogoutRequest());
				in.close();
				out.close();
				socket.close();
				if (pollSocket != null && !pollSocket.isClosed()) {
					pollOut.close();
					pollIn.close();
					pollSocket.close();
				}
				listenThread.interrupt();
				System.out.println(logger.showLogs());
			}
		} catch (IOException ignored) {
		}
	}

	private Object sendAndWait(Object request) throws IOException, InterruptedException {
		out.writeObject(request);
		out.flush();
		var resp = incomingResponses.take();
		logger.logInfo("Sync response - " + resp.getClass().getSimpleName());
		return resp;
	}

	private void sendAsync(Object request) throws IOException {
		out.writeObject(request);
		out.flush();
	}

	private boolean register(String name, String password) throws IOException, InterruptedException {
		Object resp = sendAndWait(new RegistrationRequest(name, password));
		if (resp instanceof AuthorisationResponse(Integer userId)) {
			return userId != -1;
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
		private boolean authPoll(String name, String password) throws IOException {
			pollOut.writeObject(new AuthPollRequest(name, password));
			pollOut.flush();
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
					pollOut.writeObject(new PollRequest());
					pollOut.flush();
					Object response = pollIn.readObject();
					handlePollResponse(response);
				}
			} catch (Exception e) {
				if (running) logger.logError("Poll error: " + e.getMessage());
			}
		}

		private void handlePollResponse(Object response) {
			switch (response) {
				case GetMessagesResponse(List<Message> messages) -> {
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
				case SendMessageResponse(long realId) -> {
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
				case MarkAsSeenResponse(long msgId, LocalDateTime seenTime) -> {
					for (Message msg : currentDialogue) {
						if (msg.from().equals(currentUser.name()) && msg.messageId() == msgId) {
							msg.setSeenTime(seenTime);
							redrawChatScreen();
							break;
						}
					}
				}
				case InfoMessage(String msg) -> logger.logInfo("Server Poll: " + msg);
				case ErrorMessage(String msg) -> logger.logError("Server Poll Error: " + msg);
				default -> logger.logError("Unexpected poll response: " + response);
			}
		}

		public void logout() {
			try {
				pollOut.writeObject(new LogoutRequest());
				pollOut.flush();
			} catch (IOException e) {
				logger.logError("Poller Error:", e.getMessage());
			}
		}
	}

	Poller poller = new Poller();

	/**
	 * Авторизация существующего пользователя.
	 * При успехе сохраняет currentUser и запрашивает список активных пользователей.
	 */
	@SuppressWarnings(value = "BooleanMethodIsAlwaysInverted")
	private boolean login(String name, String password) throws IOException, InterruptedException {
		Object resp = sendAndWait(new AuthorisationRequest(name, password));
		if (resp instanceof AuthorisationResponse(Integer userId) && userId != -1) {
			if (poller.authPoll(name, password)) {
				currentUser = new User(userId, name);
				System.out.println("Login successful as " + name);
				System.out.println("Welcome to Messenger!");
				System.out.println("Print /help to see commands");
				showActiveUsers();
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
	private void showActiveUsers() throws IOException, InterruptedException {
		Object resp = sendAndWait(new GetNamesRequest());
		if (resp instanceof GetNamesResponse(List<User> users)) {
			if (users.isEmpty()) {
				System.out.println("No other active users.");
			} else {
				System.out.println("Active users:");
				for (int i = 0; i < users.size(); i++) {
					System.out.println((i + 1) + ". " + users.get(i).name());
				}
			}
		} else if (resp instanceof ErrorMessage(String message)) {
			logger.logError(message);
		}
	}

	/**
	 * Открыть чат с выбранным пользователем.
	 * - Загружает диалог (все сообщения между пользователями).
	 * - Отмечает все непрочитанные сообщения от этого пользователя как прочитанные.
	 * - Переходит в режим чата (chatPartner = with).
	 */
	private void openChat(String with) throws IOException, InterruptedException {
		if (with.equals(currentUser.name())) {
			logger.logInfo("You cannot chat with yourself.");
			return;
		}
		Object resp = sendAndWait(new GetDialogRequest(with));
		if (resp instanceof GetMessagesResponse(List<Message> dialog)) {
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
	private void sendMessageToChat(String text) throws IOException {
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
	private void broadcast(String text) throws IOException {
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

	//TODO переписать это спагетти к чертям
	/// консоль. спагетти
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
							showActiveUsers();
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
					} else if(input.equals("/help")){
						System.out.println(getHelp());
					}else if (!input.isEmpty()) {
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

	/**
	 * Точка входа
	 * поддерживаются команды для консольного вызова
	 * java -jar client.jar [ФЛАГИ]
	 * Флаги:
	 * -i {ip:port} адрес сервера
	 * -r логин для РЕГИСТРАЦИИ
	 * -l логин для АВТОРИЗАЦИИ (-r и -l не быть существовать вместе)
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

		try {
			Client client = new Client(host, port);
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