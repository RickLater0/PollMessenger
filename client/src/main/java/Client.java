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
import java.util.concurrent.TimeUnit;

public final class Client {
	private final Socket socket;
	private final ObjectInputStream in;
	private final ObjectOutputStream out;

	private final Logger logger = new Logger();
	
	private final Thread listenThread;

	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

	private volatile boolean running = true;
	private User currentUser = null;
	private User chatPartner = null;

	private final BlockingQueue<Object> incomingResponses = new LinkedBlockingQueue<>();

	List<Message> currentDialogue = new CopyOnWriteArrayList<>();

	public Client(String address, int port) throws IOException {
		socket = new Socket(address, port);
		in = new ObjectInputStream(socket.getInputStream());
		out = new ObjectOutputStream(socket.getOutputStream());
		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
		listenThread = new Thread(this::listening);
		listenThread.setDaemon(true);
		listenThread.start();
	}

	void listening(){
		try {
			while (running && !socket.isClosed()) {
				Object response = in.readObject();
				if(!incomingResponses.offer(response))
					logger.logError("Couldn't offer");
			}
		} catch (EOFException e) {
			//ignore
		} catch (IOException | ClassNotFoundException e) {
			if (running) System.err.println("Connection error: " + e.getMessage());
		} finally {
			close();
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
				listenThread.interrupt();
			}
		} catch (IOException ignored) {}
	}

	private Object sendAndWait(Object request) throws IOException, InterruptedException {
		out.writeObject(request);
		out.flush();
		return incomingResponses.take();
	}

	private void sendAsync(Object request) throws IOException {
		out.writeObject(request);
		out.flush();
	}

	private boolean register(String name, String password) throws IOException, InterruptedException {
		Object resp = sendAndWait(new RegistrationRequest(name, password));
		if (resp instanceof AuthorisationResponse(boolean success)) {
			return success;
		}
		return false;
	}

	/**
	 * Авторизация существующего пользователя.
	 * При успехе сохраняет currentUser и запрашивает список активных пользователей.
	 */
	private boolean login(String name, String password) throws IOException, InterruptedException {
		Object resp = sendAndWait(new AuthorisationRequest(name, password));
		if (resp instanceof AuthorisationResponse(boolean success) && success) {
			currentUser = new User(name);
			System.out.println("Login successful as " + name);
			System.out.println("Welcome to Messenger Client!");
			showActiveUsers();
			return true;
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
	private void openChat(User with) throws IOException, InterruptedException {
		if (with.equals(currentUser)) {
			logger.logInfo("You cannot chat with yourself.");
			return;
		}
		Object resp = sendAndWait(new GetDialogRequest(with));
		if (resp instanceof GetMessagesResponse(List<Message> dialog)) {
			currentDialogue.clear();
			currentDialogue.addAll(dialog);
			chatPartner = with;

			for (Message msg : currentDialogue) {
				if (msg.to() != null && msg.to().equals(currentUser) && msg.from().equals(with) && msg.seenTime() == null) {
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

		// Временное сообщение (отрицательный ID, чтобы не путать с реальными)
		long tempId = -System.currentTimeMillis();
		Message tempMsg = new Message(tempId, new MessageContent(text), currentUser, chatPartner, LocalDateTime.now(), null);
		currentDialogue.add(tempMsg);
		redrawChatScreen();

		// Реальная отправка (асинхронно, не ждём ответа для плавности)
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
		// Очистка экрана (ANSI escape codes — работает в большинстве терминалов)
		clearConsole();
		System.out.println("=== Chat with " + chatPartner.name() + " ===");
		if (currentDialogue.isEmpty()) {
			System.out.println("No messages yet.");
		} else {
			for (Message msg : currentDialogue) {
				String sender = msg.from().equals(currentUser) ? "You" : msg.from().name();
				String time = msg.dispatchTime().format(TIME_FORMATTER);

				if (msg.seenTime() != null) {
					System.out.printf("[%s>%s] %s: %s%n", time, msg.seenTime().format(TIME_FORMATTER), sender, msg.content().content());
				}else {
					if(msg.messageId() < 0)
						System.out.printf("[%s>... ] %s: %s%n", time, sender, msg.content().content());
					else
						System.out.printf("[%s>sent] %s: %s%n", time, sender, msg.content().content());
				}
			}
		}
	}

	/**
	 * Запустить отдельный поток для long polling.
	 * Этот поток будет постоянно отправлять PollRequest и получать новые сообщения.
	 * Новые сообщения обрабатываются здесь же и, если они от текущего chatPartner,
	 * автоматически отмечаются как прочитанные и выводятся.
	 */
	private void startPolling() {
		Thread pollThread = new Thread(() -> {
			try {
				while (running && currentUser != null && !socket.isClosed()) {
					// Отправляем запрос на получение сообщений (тайм-аут 30 сек)
					sendAsync(new PollRequest(currentUser));
					// Ждём ответа (он придёт в responseReader, но мы его перехватим через очередь)
					Object response = incomingResponses.poll(35, TimeUnit.SECONDS);
					if (response instanceof GetMessagesResponse(List<Message> messages)) {
						for (Message msg : messages) {
							// Если мы в чате с отправителем и сообщение адресовано нам (или broadcast)
							if (chatPartner != null && chatPartner.equals(msg.from())) {
								currentDialogue.add(msg);
								sendAsync(new MarkAsSeenRequest(msg.messageId()));
								msg.setSeenTime(LocalDateTime.now()); // локально помечаем прочитанным
								redrawChatScreen();
							} else if (chatPartner == null && (msg.to() == null || msg.to().equals(currentUser))) {
								System.out.print("\n[New" + msg.from().name() + "]");
								System.out.print("\n> ");
							}
						}
					}else if (response instanceof SendMessageResponse(int realId)) {
						// Ищем временное сообщение от текущего пользователя с отрицательным ID
						for (Message msg : currentDialogue) {
							if (msg.messageId() < 0 && msg.from().equals(currentUser) && msg.to().equals(chatPartner)) {
								currentDialogue.remove(msg);
								Message updated = new Message(realId, msg.content(), msg.from(), msg.to(),
										msg.dispatchTime(), msg.seenTime());
								currentDialogue.add(updated);
								redrawChatScreen();
								break;
							}
						}
					}else if (response instanceof MarkAsSeenResponse(long msgId, LocalDateTime seenTime)) {
						// Отправитель получает уведомление, что его сообщение прочитано
						for (Message msg : currentDialogue) {
							if (msg.messageId() == msgId && msg.from().equals(currentUser)) {
								msg.setSeenTime(seenTime);
								redrawChatScreen();
								break;
							}
						}
					}
				}
			} catch (InterruptedException | IOException e) {
				if (running) logger.logError("Error:", e.getMessage());
			}
		});
		pollThread.setDaemon(true);
		pollThread.start();
	}

	private void console() {
		Scanner scanner = new Scanner(System.in);
		boolean exit = false;


		while (!exit && running) {
			if (currentUser == null) {
				// Не авторизованы – показываем меню входа/регистрации
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
							if (login(name, pass)) {
								startPolling();
							}
						} catch (Exception e) {
							System.err.println("Login error: " + e.getMessage());
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
				// Авторизованы – режим выбора действия
				if (chatPartner == null) {
					// Не в чате
					System.out.println("\nCommands:");
					System.out.println("  /users          - show active users");
					System.out.println("  /chat <name>    - open chat with user");
					System.out.println("  /broadcast <msg> - send message to everyone");
					System.out.println("  /logout         - logout");
					System.out.println("  /exit           - quit client");
					System.out.print("> ");
					String input = scanner.nextLine().trim();
					if (input.startsWith("/users")) {
						try { showActiveUsers(); } catch (Exception e) { logger.logError("Error:", e.getMessage()); }
					} else if (input.startsWith("/chat ")) {
						String target = input.substring(6).trim();
						if (!target.isEmpty()) {
							try { openChat(new User(target)); } catch (Exception e) { logger.logError("Error:", e.getMessage()); }
						} else {
							System.out.println("Usage: /chat username");
						}
					} else if (input.startsWith("/broadcast ")) {
						String msg = input.substring(11).trim();
						if (!msg.isEmpty()) {
							try { broadcast(msg); } catch (IOException e) { logger.logError("Error:", e.getMessage()); }
						}
					} else if (input.equals("/logs show")) {
						System.out.println(logger.showLogs());
					} else if (input.equals("/logs clear")) {
						System.out.println("Cleared");
						logger.clearLogs();
					}else if (input.equals("/logout")) {
						try {
							sendAsync(new LogoutRequest());
							currentUser = null;
							chatPartner = null;
							System.out.println("Logged out.");
						} catch (IOException e) { logger.logError("Error:", e.getMessage()); }
					} else if (input.equals("/exit")) {
						exit = true;
					} else {
						System.out.println("Unknown command. Type /users, /chat, /broadcast, /logout, /exit");
					}
				} else {
					// В режиме чата
					System.out.print("[c:" + chatPartner.name() + "] > ");
					String input = scanner.nextLine().trim();
					if (input.equals("/exit")) {
						exitChat();
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


	static void main(String[] args) {
		// Параметры по умолчанию
		String host = "127.0.0.1";
		int port = 43500;
		String loginName = null;
		String loginPassword = null;

		// Разбор аргументов вида -s host:port -c user -p pass
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-s":
					if (i + 1 < args.length) {
						String serverArg = args[++i];
						String[] parts = serverArg.split(":");
						host = parts[0];
						if (parts.length > 1) {
							try { port = Integer.parseInt(parts[1]); } catch (NumberFormatException ignored) {}
						}
					}
					break;
				case "-c":
					if (i + 1 < args.length) loginName = args[++i];
					break;
				case "-p":
					if (i + 1 < args.length) loginPassword = args[++i];
					break;
			}
		}

		// Запрос адреса сервера, если не задан через -s
		if (host.equals("127.0.0.1") && port == 43500 && args.length == 0) {
			Scanner scanner = new Scanner(System.in);
			System.out.print("Server address (ip:port) [127.0.0.1:43500]: ");
			String input = scanner.nextLine().trim();
			if (!input.isEmpty()) {
				String[] parts = input.split(":");
				host = parts[0];
				if (parts.length > 1) {
					try { port = Integer.parseInt(parts[1]); } catch (NumberFormatException ignored) {}
				}
			}
		}

		try {
			Client client = new Client(host, port);
			// Если указаны и логин, и пароль — выполняем автоматический вход
			if (loginName != null && loginPassword != null) {
				if (client.login(loginName, loginPassword)) {
					client.console();
				} else {
					client.logger.logError("Auto-login failed. Starting manual mode.");
					client.console();
				}
			} else {
				client.console();
			}
		} catch (IOException | InterruptedException e) {
			System.err.println("Cannot connect to server: " + e.getMessage());
		}
	}

}