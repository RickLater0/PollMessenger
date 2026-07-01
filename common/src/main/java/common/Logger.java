package common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Logger {

	protected static class CColors {
		// Сброс
		public static final String RESET = "\033[0m";

		// Обычные цвета текста
		public static final String BLACK = "\033[0;30m";
		public static final String RED = "\033[0;31m";
		public static final String GREEN = "\033[0;32m";
		public static final String YELLOW = "\033[0;33m";
		public static final String BLUE = "\033[0;34m";
		public static final String PURPLE = "\033[0;35m";
		public static final String CYAN = "\033[0;36m";
		public static final String WHITE = "\033[0;37m";

		// Жирные цвета
		public static final String BOLD_BLACK = "\033[1;30m";
		public static final String BOLD_RED = "\033[1;31m";
		public static final String BOLD_GREEN = "\033[1;32m";
		// ... и так далее

		// Фон
		public static final String BG_BLACK = "\033[40m";
		public static final String BG_RED = "\033[41m";
		// ...
	}

	protected record LogEntry(LocalDateTime timestamp, LogLevel level, String message, String stackTrace) {
		LogEntry(LogLevel level, String message, String stackTrace){
			this(LocalDateTime.now(), level, message, stackTrace);
		}

		LogEntry(LogLevel level, String message){
			this(LocalDateTime.now(), level, message, null);
		}

		public String format() {
			String time = timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
			if (!isColorSupported()) {
				return String.format("[%s] %s: %s%s", time, level, message,
						(stackTrace != null ? "\n\t" + stackTrace : ""));
			}
			var messageC = CColors.WHITE;
			var resetC = CColors.RESET;
			switch (level) {
				case ERROR -> messageC = CColors.RED;
				case INFO -> messageC = CColors.CYAN;
				case WARNING -> messageC = CColors.YELLOW;
				case DEBUG -> messageC = CColors.BOLD_GREEN;
			}
			if (stackTrace != null && !stackTrace.isEmpty()) {
				return String.format("[%s] %s%s%s: %s\n\t%s", time, messageC, level, resetC, message, stackTrace);
			}
			return String.format("[%s] %s%s%s: %s", time, messageC, level, resetC, message);
		}
	}

	private static boolean isColorSupported() {
		if (System.getenv("NO_COLOR") != null) return false;

		String os = System.getProperty("os.name").toLowerCase();
		if (os.contains("win")) {
			return System.console() != null;
		}
		return true;
	}

	public enum LogLevel {INFO, ERROR, WARNING, DEBUG}

	private final List<LogEntry> logs = new CopyOnWriteArrayList<>();

	private void logLevel(LogLevel level, String message) {
		logs.add(new LogEntry(level, message));
	}

	public void logInfo(String info){
		logLevel(LogLevel.INFO, info);
	}

	public void logError(String info, String err){
		logs.add(new LogEntry(LogLevel.ERROR, info, err));
	}

	public void logError(String info){
		logLevel(LogLevel.ERROR, info);
	}

	public void logWarning(String info) {
		logLevel(LogLevel.WARNING, info);
	}

	public void logDebug(String info) {
		logLevel(LogLevel.DEBUG, info);
	}

	public String showLogs(){
		StringBuilder res = new StringBuilder();
		for(var entry : logs){
			res.append(entry.format()).append("\n");
		}
		return res.toString().isBlank() ? "NO LOGS" : res.toString();
	}

	public String showInfoLogs() {
		return showCertainLogs(LogLevel.INFO);
	}

	public String showErrorLogs() {
		return showCertainLogs(LogLevel.ERROR);
	}

	public String showWarningLogs() {
		return showCertainLogs(LogLevel.WARNING);
	}

	public String showDebugLogs() {
		return showCertainLogs(LogLevel.DEBUG);
	}

	private String showCertainLogs(LogLevel level) {
		StringBuilder res = new StringBuilder();
		for (var entry : logs) {
			if (entry.level == level) res.append(entry.format()).append("\n");
		}
		return res.toString().isBlank() ? ("NO " + level + " LOGS") : res.toString();
	}

	public String getLastLog() {
		return logs.getLast().format();
	}

	public void clearLogs(){
		logs.clear();
	}
	public void clearInfo(){
		logs.removeIf(log -> log.level == LogLevel.INFO);
	}
	public void clearErrors(){
		logs.removeIf(log -> log.level == LogLevel.ERROR);
	}

	public void clearDebug() {
		logs.removeIf(log -> log.level == LogLevel.DEBUG);
	}

	public void clearWarnings() {
		logs.removeIf(log -> log.level == LogLevel.WARNING);
	}

	private void clearCertainLogs(LogLevel level) {
		logs.removeIf(log -> log.level == level);

	}
}

