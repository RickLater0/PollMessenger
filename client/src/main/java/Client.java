import common.dto.*;
import common.messages.*;
import common.requests.*;
import common.responses.*;

import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Client {
	private final String serverHost;
	private final int serverPort;

	private final Socket cmdSocket;
	private final ObjectOutputStream cmdOut;
	private final ObjectInputStream cmdIn;

	private Socket pollSocket;
	private ObjectOutputStream pollOut;
	private ObjectInputStream pollIn;

	private User currentUser;
	private final AtomicBoolean running = new AtomicBoolean(true);
	private Thread pollerThread;

	private User activeChatWith = null;
	private boolean inChat = false;
	private final List<Message> currentChatHistory = new ArrayList<>();

	public Client(String host, int port) throws IOException {
		this.serverHost = host;
		this.serverPort = port;
		cmdSocket = new Socket(serverHost, serverPort);
		cmdOut = new ObjectOutputStream(cmdSocket.getOutputStream());
		cmdIn = new ObjectInputStream(cmdSocket.getInputStream());
	}

	// ---------- Базовые команды ----------
	private boolean register(String username, String password) throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new RegistrationRequest(username, password));
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof AuthorisationResponse(boolean success)) {
			return success;
		} else if (response instanceof ErrorMessage(String message)) {
			System.err.println("Registration failed: " + message);
		}
		return false;
	}

	private boolean login(String username, String password) throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new AuthorisationRequest(username, password));
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof AuthorisationResponse(boolean success)) {
			if (success) {
				currentUser = new User(username);
				return true;
			} else {
				System.err.println("Login failed: invalid credentials");
			}
		} else if (response instanceof ErrorMessage(String message)) {
			System.err.println("Login error: " + message);
		}
		return false;
	}

	private List<User> getActiveUsers() throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new GetNamesRequest());
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof GetNamesResponse(List<User> users)) {
			return users;
		} else if (response instanceof ErrorMessage(String message)) {
			System.err.println("Cannot get user list: " + message);
		}
		return Collections.emptyList();
	}

	private List<Message> getDialogWith(User partner) throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new GetDialogRequest(partner));
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof GetMessagesResponse(List<Message> messages)) {
			return messages;
		} else if (response instanceof ErrorMessage(String message)) {
			System.err.println("Cannot get dialog: " + message);
		}
		return Collections.emptyList();
	}

	private void sendMessage(String content, User to) throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new SendMessageRequest(new MessageContent(content), to));
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof ErrorMessage(String message)) {
			System.err.println("Send failed: " + message);
		} else {
			// Добавляем отправленное сообщение в локальную историю
			Message sentMsg = new Message(0, new MessageContent(content), currentUser, to, LocalDateTime.now(), null);
			currentChatHistory.add(sentMsg);
			if (inChat && activeChatWith != null && activeChatWith.equals(to)) {
				redrawChat();
			}
		}
	}

	private void sendBroadcast(String content) throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new SendMessageRequest(new MessageContent(content), null));
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof ErrorMessage(String message)) {
			System.err.println("Broadcast failed: " + message);
		} else {
			System.out.println("Broadcast sent");
		}
	}

	private void markAsSeen(long messageId, User chatter) throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new MarkAsSeenRequest(messageId,chatter));
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof ErrorMessage(String message)) {
			System.err.println("Failed to mark message: " + message);
		}
	}

	private void logout() throws IOException, ClassNotFoundException {
		cmdOut.writeObject(new LogoutRequest());
		cmdOut.flush();
		Object response = cmdIn.readObject();
		if (response instanceof InfoMessage) {
			System.out.println("Logout successful");
		} else if (response instanceof ErrorMessage(String message)) {
			System.err.println("Logout error: " + message);
		}
		running.set(false);
		if (pollerThread != null) pollerThread.interrupt();
		close();
	}

	private void close() {
		try {
			if (cmdIn != null) cmdIn.close();
			if (cmdOut != null) cmdOut.close();
			if (cmdSocket != null) cmdSocket.close();
			if (pollIn != null) pollIn.close();
			if (pollOut != null) pollOut.close();
			if (pollSocket != null) pollSocket.close();
		} catch (IOException e) {
			System.err.println("Error closing connection: " + e.getMessage());
		}
	}

	// ---------- Long polling поток ----------
	private class Poller implements Runnable {
		@Override
		public void run() {
			try {
				pollSocket = new Socket(serverHost, serverPort);
				pollOut = new ObjectOutputStream(pollSocket.getOutputStream());
				pollIn = new ObjectInputStream(pollSocket.getInputStream());

				while (running.get() && !Thread.currentThread().isInterrupted()) {
					pollOut.writeObject(new PollRequest(currentUser));
					pollOut.flush();
					Object response = pollIn.readObject();
					if (response instanceof GetMessagesResponse(List<Message> messages)) {
						for (Message msg : messages) {
							// Если мы в чате с отправителем
							if (inChat && activeChatWith != null && activeChatWith.equals(msg.from())) {
								boolean found = false;
								for (int i = 0; i < currentChatHistory.size(); i++) {
									if (currentChatHistory.get(i).messageId() == msg.messageId()) {
										currentChatHistory.set(i, msg);
										found = true;
										break;
									}
								}
								if (!found) {
									currentChatHistory.add(msg);
								}
								if (msg.seenTime() == null) {
									markAsSeen(msg.messageId(), activeChatWith);

									for (Message m : currentChatHistory) {
										if (m.messageId() == msg.messageId() && m.seenTime() == null) {
											// Создаём копию с текущим временем
											try {
												Message updated = new Message(m.messageId(), m.content(), m.from(), m.to(),
														m.dispatchTime(), LocalDateTime.now());
												int idx = currentChatHistory.indexOf(m);
												if (idx != -1) currentChatHistory.set(idx, updated);
											} catch (Exception e) {
												// ignore
											}
											break;
										}
									}
								}
								// Перерисовываем чат
								redrawChat();
							}
						}
					} else if (response instanceof ErrorMessage(String message)) {
						if (message.contains("not authorised")) {
							System.err.println("Polling error: not authorised. Stopping poller.");
							break;
						}
					}
				}
			} catch (IOException | ClassNotFoundException e) {
				if (running.get()) {
					System.err.println("Polling error: " + e.getMessage());
				}
			} finally {
				try {
					if (pollIn != null) pollIn.close();
					if (pollOut != null) pollOut.close();
					if (pollSocket != null) pollSocket.close();
				} catch (IOException ignored) {}
			}
		}
	}

	private void startPoller() {
		pollerThread = new Thread(new Poller());
		pollerThread.setDaemon(true);
		pollerThread.start();
	}

	// ---------- Отображение чата ----------
	private void redrawChat() {
		clearConsole();
		if (activeChatWith == null) return;
		System.out.println("\n=== Chat with " + activeChatWith.name() + " ===");
		if (currentChatHistory.isEmpty()) {
			System.out.println("No messages yet.");
		} else {
			currentChatHistory.sort(Comparator.comparing(Message::dispatchTime));
			for (Message msg : currentChatHistory) {
				String from = msg.from().name();
				String content = msg.content().content();
				String sent = formatShortDateTime(msg.dispatchTime());
				String seen;
				if (msg.from().equals(currentUser) && msg.seenTime() == null) {
					seen = "(sent)";
				} else if (msg.seenTime() != null) {
					seen = formatShortDateTime(msg.seenTime());
				} else {
					seen = "-";
				}
				System.out.printf("%s %s - %s : %s\n", from, sent, seen, content);
			}
		}
		System.out.print(activeChatWith.name() + "> ");
	}

	private void showActiveUsers() {
		try {
			List<User> users = getActiveUsers();
			System.out.println("\n=== Active users ===");
			if (users.isEmpty()) {
				System.out.println("  (no other active users)");
			} else {
				users.forEach(u -> System.out.println("  " + u.name()));
			}
		} catch (IOException | ClassNotFoundException e) {
			System.err.println("Cannot fetch user list: " + e.getMessage());
		}
	}

	private void openChat(User partner) {
		try {
			List<Message> history = getDialogWith(partner);
			// Отмечаем непрочитанные сообщения от партнёра на сервере
			for (Message msg : history) {
				if (msg.from().equals(partner) && msg.seenTime() == null) {
					markAsSeen(msg.messageId(), activeChatWith);
				}
			}
			// Сохраняем историю с уже обновлёнными seenTime (сервер вернул исходное, но после markAsSeen обновится в БД,
			// однако нам для отображения можно использовать те же объекты, так как seenTime ещё не обновлён локально.)
			// Чтобы не ждать, можно сразу после markAsSeen установить seenTime, но проще довериться следующему poll.
			// Однако для немедленного отображения прочтения в текущей сессии – обновим локальные копии:
			for (Message msg : history) {
				if (msg.from().equals(partner) && msg.seenTime() == null) {
					try {
						// Создаём новое сообщение с текущим временем (имитация прочтения)
						Message updated = new Message(msg.messageId(), msg.content(), msg.from(), msg.to(),
								msg.dispatchTime(), LocalDateTime.now());
						int idx = history.indexOf(msg);
						if (idx != -1) history.set(idx, updated);
					} catch (Exception e) {
						//ignore
					}
				}
			}
			currentChatHistory.clear();
			currentChatHistory.addAll(history);
			activeChatWith = partner;
			inChat = true;
			redrawChat();
		} catch (IOException | ClassNotFoundException e) {
			System.err.println("Failed to open chat: " + e.getMessage());
		}
	}

	private void closeChat() {
		activeChatWith = null;
		inChat = false;
		currentChatHistory.clear();
		clearConsole();
		showActiveUsers();
	}

	private String formatShortDateTime(LocalDateTime dt) {
		return String.format("%02d.%02d %02d:%02d",
				dt.getDayOfMonth(), dt.getMonthValue(),
				dt.getHour(), dt.getMinute());
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

	private void printHelp() {
		System.out.println("\n=== COMMANDS ===");
		System.out.println("Outside chat:");
		System.out.println("  users                - show active users");
		System.out.println("  chat <username>      - open chat with user");
		System.out.println("  sendall <text>       - send broadcast message");
		System.out.println("  logout               - logout and exit");
		System.out.println("  exit                 - exit client");
		System.out.println("  help                 - show this help");
		System.out.println("\nInside chat:");
		System.out.println("  <any text>           - send message to current chat partner");
		System.out.println("  closechat            - close current chat and return to users list");
		System.out.println("  help                 - show this help");
		System.out.println();
	}

	// ---------- Основной цикл ----------
	private void run(Scanner scanner) {
		boolean authenticated = false;

		while (!authenticated && running.get()) {
			System.out.print("reg/login <user> <pass>: ");
			String line = scanner.nextLine().trim();
			if (line.isEmpty()) continue;
			String[] parts = line.split("\\s+");
			String cmd = parts[0].toLowerCase();

			try {
				if (cmd.equals("reg") && parts.length == 3) {
					if (register(parts[1], parts[2])) {
						System.out.println("Registration successful. You are now logged in.");
						authenticated = true;
						startPoller();
					} else {
						System.out.println("Registration failed. Username may already exist.");
					}
				} else if (cmd.equals("login") && parts.length == 3) {
					if (login(parts[1], parts[2])) {
						System.out.println("Login successful.");
						authenticated = true;
						startPoller();
					} else {
						System.out.println("Login failed.");
					}
				} else {
					System.out.println("Invalid authentication command. Use: reg <user> <pass> or login <user> <pass>");
				}
			} catch (IOException | ClassNotFoundException e) {
				System.err.println("Connection error: " + e.getMessage());
				running.set(false);
				break;
			}
		}

		if (!authenticated) return;

		System.out.println("Use 'help' to see all commands");
		showActiveUsers();

		while (running.get()) {
			if (!inChat) {
				System.out.print("> ");
			}
			String line = scanner.nextLine().trim();
			if (line.isEmpty()) continue;

			if (inChat) {
				if (line.equalsIgnoreCase("closechat")) {
					closeChat();
				} else if (line.equalsIgnoreCase("help")) {
					printHelp();
					if (inChat && activeChatWith != null) {
						System.out.print(activeChatWith.name() + "> ");
					}
				} else if (line.equalsIgnoreCase("logout") || line.equalsIgnoreCase("exit")) {
					try {
						logout();
					} catch (IOException | ClassNotFoundException e) {
						System.err.println(e.getMessage());
					}
					running.set(false);
					break;
				} else {
					try {
						sendMessage(line, activeChatWith);
					} catch (IOException | ClassNotFoundException e) {
						System.err.println("Send error: " + e.getMessage());
						if (inChat && activeChatWith != null) {
							System.out.print(activeChatWith.name() + "> ");
						}
					}
				}
			} else {
				String[] parts = line.split("\\s+");
				String cmd = parts[0].toLowerCase();

				try {
					switch (cmd) {
						case "users":
							showActiveUsers();
							break;
						case "chat":
							if (parts.length < 2) {
								System.out.println("Usage: chat <username>");
								break;
							}
							String partner = parts[1];
							if (partner.equals(currentUser.name())) {
								System.out.println("You cannot chat with yourself.");
								break;
							}
							openChat(new User(partner));
							break;
						case "sendall":
							if (parts.length < 2) {
								System.out.println("Usage: sendall <message>");
								break;
							}
							String msgText = line.substring(7).trim();
							if (msgText.isEmpty()) {
								System.out.println("Message cannot be empty.");
								break;
							}
							sendBroadcast(msgText);
							break;
						case "logout":
							logout();
							running.set(false);
							break;
						case "exit":
							System.out.println("Exiting...");
							running.set(false);
							System.exit(0);
							return;
						case "help":
							printHelp();
							break;
						default:
							System.out.println("Unknown command. Type 'help' for available commands.");
					}
				} catch (IOException | ClassNotFoundException e) {
					System.err.println("Command error: " + e.getMessage());
					running.set(false);
				}
			}
		}
		close();
	}

	static void main() {
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.print("Enter server host (default 127.0.0.1): ");
			String host = scanner.nextLine().trim();
			if (host.isEmpty()) host = "127.0.0.1";

			System.out.print("Enter server port (default 43500): ");
			String portStr = scanner.nextLine().trim();
			int port = 43500;
			if (!portStr.isEmpty()) {
				try {
					port = Integer.parseInt(portStr);
				} catch (NumberFormatException e) {
					System.out.println("Invalid port, using default 43500");
				}
			}

			try {
				Client client = new Client(host, port);
				client.run(scanner);
			} catch (IOException e) {
				System.err.println("Cannot connect to server: " + e.getMessage());
				System.out.println("Retrying...");
				continue;
			}
			System.out.println("Disconnected. Starting over.\n");
		}
	}
}