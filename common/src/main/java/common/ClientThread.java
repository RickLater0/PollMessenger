package common;

import common.dto.User;

import java.net.*;
import java.io.*;

public class ClientThread extends Thread{

	private final Socket clientSocket;

	private User user;

	public ClientThread(String name, Socket socket){
		this.user = new User(name);
		this.clientSocket = socket;
	}

	public ClientThread(Socket socket){
		this("@UNREGISTERED", socket);
	}

	@Override
	public void run(){
		try (ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream())) {
			try (ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream()))
			{
				
			}
			catch (IOException e){
				throw new RuntimeException(e);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	public User getUser(){
		return this.user;
	}
}
