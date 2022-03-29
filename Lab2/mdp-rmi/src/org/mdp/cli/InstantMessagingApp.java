package org.mdp.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.mdp.RMIUtils;
import org.mdp.dir.User;
import org.mdp.dir.UserDirectoryServer;
import org.mdp.dir.UserDirectoryStub;
import org.mdp.im.InstantMessagingServer;
import org.mdp.im.InstantMessagingStub;


public class InstantMessagingApp {

	static String LOGO = 
			"========================================\n"+
					" _ __ ___   ___ _ __  ___  __ _ (_) ___\n"+ 
					"| '_ ` _ \\ / _ \\ '_ \\/ __|/ _` || |/ _ \n"+
					"| | | | | |  __/ | | \\__ \\ (_| || |  __/\n"+
					"|_| |_| |_|\\___|_| |_|___/\\__,_|/ |\\___|\n"+
					"                               |__/     \n"+
					"========================================\n";

	static String INTRO = LOGO +"... vivir mejor conectado\n";

	static String LIST = "list";
	static String ADD = "add";
	static String RM = "rm";
	static String MSG = "msg";
	static String ALL = "all";

	static String HR = "========================================";

	static TreeSet<String> RESTRICTED;
	static {
		RESTRICTED = new TreeSet<String>();
		RESTRICTED.add(ADD);
		RESTRICTED.add(RM);
		RESTRICTED.add(MSG);
		RESTRICTED.add(ALL);
		RESTRICTED.add("");
	}

	/** 
	 * @param args
	 * @throws IOException
	 * @throws AlreadyBoundException
	 * @throws NotBoundException 
	 */
	public static void main(String args[]) throws IOException, AlreadyBoundException, NotBoundException{
		Option hostnameO = new Option("n", "hostname or ip of directory (defaults to localhost)");
		hostnameO.setArgs(1);

		Option portO = new Option("p", "port of directory (defaults to "+RMIUtils.DEFAULT_REG_PORT+")");
		portO.setArgs(1);

		Option helpO = new Option("h", "print help (e.g., to see server types)");

		Options options = new Options();
		options.addOption(hostnameO);
		options.addOption(portO);
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			printHelp(options);
			return;
		}

		// print help options and return
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}

		// set hostname ... if null, same as localhost
		String dir_host = null;
		if (cmd.hasOption("n")) {
			dir_host = cmd.getOptionValue("n");
		}

		int dir_port = RMIUtils.DEFAULT_REG_PORT;
		if (cmd.hasOption("p")) {
			dir_port = Integer.parseInt(cmd.getOptionValue("p"));
		}

		System.out.println(INTRO);
		
		// Connect to the directory
		UserDirectoryStub uds = connectToDirectory(dir_host, dir_port);
		Map<String,User> dir = uds.getDirectory();

		// open for user input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		String uname, name, uhost;
		int uport;

		while(true){
			System.out.println("\n\n> First enter a username:");
			String line = br.readLine().trim();
			if(RESTRICTED.contains(line)){
				System.err.println("\n\n!! Restricted username ... try another !!");
			} else{
				uname = line;
				break;
			}
		}

		while(true){
			System.out.println("\n\n> Next enter your real name:");
			String line = br.readLine().trim();
			if(RESTRICTED.contains(line)){
				System.err.println("\n\n!! Restricted username ... try another !!");
			} else{
				name = line;
				break;
			}
		}

		while(true){
			System.out.println("\n\n> Next your hostname or local IP:");
			String line = br.readLine().trim();
			uhost = line;
			break;
		}
		
		// we keep 1985 for the directory
		while(true){
			System.out.println("\n\n> Finally your port (any number between 1001 and 1899):");
			String line = br.readLine().trim();
			try{
				uport = Integer.parseInt(line);
				if(uport>1000 && uport<1900){
					break;
				} else{
					System.err.println("\n\n!! Invalid port (should be between 1001 and 1899) !!");
				}
			} catch(Exception e){
				System.err.println("\n\n!! Not a number (should be between 1001 and 1899) !!");
			}
		}

		User me = new User(uname, name, uhost, uport);

		// Then start your own RMI registry ...
		// so people messaging you can find your server
		Registry reg = startRegistry(me.getPort());

		// and start your message server to start receiving messages ...
		// need to keep the reference to server even if not used!
		@SuppressWarnings("unused")
		Remote server = registerSkeleton(reg);

		while(true){
			System.out.println("\n\n> Type '"+LIST+"' to refresh directory, '"+ADD+"' to add yourself to the directory, '"+RM+"' to remove yourself from directory, or '"+MSG+"' to send a message:");

			String line = br.readLine().trim();

			if(line.equals(LIST)){
				dir = uds.getDirectory();
				System.out.println("\n"+HR+"\nDirectory of usernames ("+dir.size()+" users)\n"+HR);
				if(dir.isEmpty()){
					System.out.println("[empty]");
				}
				else for(String s: dir.keySet()){
					System.out.println(s+" ");
				}
				System.out.println(HR);
			} else if(line.equals(RM)){
				System.out.print("\n# Removing myself from directory ...");
				uds.removeUserWithName(me.getUsername());
				System.out.println(" [done].");
			} else if(line.equals(ADD)){
				System.out.print("\n# Adding details to directory ...");
				if(uds.createUser(me)) {
					System.out.println(" [done].");
				} else {
					System.out.println(" [failed; looks like the directory cannot find our port/IP].");
				}
			} else if(line.equals(MSG)){
				System.out.println("\n\n> Which user would you like to message (or type 'all') ...");

				line = br.readLine().trim();

				String target = line;
				ArrayList<String> usernames = new ArrayList<String>();

				if(target.equals(ALL)){
					usernames.addAll(dir.keySet());
				} else{
					usernames.add(target);
				}

				System.out.println("\n\n> Enter your message ...");
				line = br.readLine().trim();

				for(String username:usernames){

					User user = dir.get(username);
					if(user==null){
						System.err.println("\n\n!! Could not find the user "+username+" in the directory. (Type 'list' to refresh directory.) !!");
					} else{
						try{
							long ms = messageUser(user,me,line);
							System.out.println("\n\n# Message received by "+username+" at time "+new Date(ms));
						} catch(Exception e){
							System.out.println("\n\n!! Could not message user "+username+": maybe they logged off? !!");
						}
					}
				}
			} else{
				System.err.println("!! Unrecognised command "+line+" !!");
			}
		}
	}

	/**
	 * Connect to the central directory.
	 * 
	 * (See TestDirectoryClientApp for example)
	 * @param port 
	 * @param host 
	 * 
	 * 
	 * @return A stub to call message method on
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 */
	public static UserDirectoryStub connectToDirectory(String host, int port) throws RemoteException, NotBoundException {
		// open the registry on the machine
		// with the given host and port
		Registry registry = LocateRegistry.getRegistry(host, port);

		// then find the stub for the 
		// central directory that will let us
		// upload and download usernames, IPs, etc.
		// The key in the registry is given by UserDirectoryServer.DEFAULT_KEY
		// (a static variable)
		return (UserDirectoryStub) registry.lookup(UserDirectoryServer.DEFAULT_KEY);
	}

	/**
	 * Starts your registry on the given port.
	 * 
	 * @param port
	 * @return
	 * @throws RemoteException
	 * @throws AlreadyBoundException
	 */
	public static Registry startRegistry(int port) throws RemoteException, AlreadyBoundException {
		Registry registry = LocateRegistry.createRegistry(port);
		return registry;
	}

	/**
	 * Create a skeleton and bind it to the registry
	 * 
	 * @param r The local registry
	 * @return
	 * @throws RemoteException
	 * @throws AlreadyBoundException
	 */
	public static Remote registerSkeleton(Registry r) throws RemoteException, AlreadyBoundException {
		// TODO Create a new instance of that InstantMessagingServer here
		InstantMessagingServer ims = new InstantMessagingServer();

		// the skeleton is what the client connects to ...
		// Create the skeleton using UnicastRemoteObject.exportObject(.,.)
		// This creates a remotely callable interface to your local server!
		// (You can use the port 0 since the server is local.)
		Remote stub = UnicastRemoteObject.exportObject(ims, 0);

		// now bind the skeleton to the registry under a key ...
		// In this lab, we will use the static variable InstantMessagingServer.DEFAULT_KEY 
		// as the key so everyone has the same key.
		r.bind(InstantMessagingServer.DEFAULT_KEY, stub);

		// return the skeleton
		
		return stub;
	}

	/**
	 * Message user.
	 * 
	 * Connect to their registry, retrieve the InstantMessagingStub stub.
	 * Call the message method on the stub.
	 * 
	 * @param username
	 * @param msg
	 * @return Time acknowledged by message server
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws AccessException 
	 */
	public static long messageUser(User to, User from, String msg) throws AccessException, RemoteException, NotBoundException{
		//TODO first connect to the remote registry 
		// on the given host and port (given by the User to object).
		Registry registry = LocateRegistry.getRegistry(to.getHost(), to.getPort());

		// then we need to find the interface we're looking for
		// from the remote registry. We will assume that like us, they
		// registered their server with the key InstantMessagingServer.DEFAULT_KEY.
		// You should cast the result to (InstantMessagingStub) so you
		// can call methods on it.
		InstantMessagingStub stub = (InstantMessagingStub) registry.lookup(InstantMessagingServer.DEFAULT_KEY);

		// return the time given by the remote message call
		// which indicates at what time they received the message
		return stub.message(from, msg);
	}

	public static void printHelp(Options options){
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("parameters:", options);
	}
}
