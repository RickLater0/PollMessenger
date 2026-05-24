package common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Logger {
	protected record LogEntry(LocalDateTime timestamp, LogLevel level, String message, String stackTrace) {
		LogEntry(LogLevel level, String message, String stackTrace){
			this(LocalDateTime.now(), level, message, stackTrace);
		}

		LogEntry(LogLevel level, String message){
			this(LocalDateTime.now(), level, message, null);
		}

		public String format() {
			String time = timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
			if (stackTrace != null && !stackTrace.isEmpty()) {
				return String.format("[%s] %s: %s\n%s", time, level, message, stackTrace);
			}
			return String.format("[%s] %s: %s", time, level, message);
		}
	}

	public enum LogLevel { INFO, ERROR }

	private final List<LogEntry> logs = new CopyOnWriteArrayList<>();

	public void logInfo(String info){
		logs.add(new LogEntry(LogLevel.INFO, info));
	}

	public void logError(String info, String err){
		logs.add(new LogEntry(LogLevel.ERROR, info, err));
	}

	public void logError(String info){
		logs.add(new LogEntry(LogLevel.ERROR, info));
	}

	public String showLogs(){
		StringBuilder res = new StringBuilder();
		for(var entry : logs){
			res.append(entry.format()).append("\n");
		}
		return res.toString().isBlank() ? "NO LOGS" : res.toString();
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
}

