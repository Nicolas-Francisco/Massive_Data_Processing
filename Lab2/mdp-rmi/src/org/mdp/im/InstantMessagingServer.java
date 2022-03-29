package org.mdp.im;

import java.rmi.RemoteException;

import org.mdp.dir.User;
import org.mdp.dir.UserDirectoryServer;

public class InstantMessagingServer implements InstantMessagingStub {

	// default key we will use for the registry
	public static String DEFAULT_KEY = UserDirectoryServer.class.getSimpleName();
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6682365848634470441L;

	public long message(User from, String msg) throws RemoteException {
		System.out.println("\nNew Message from " + from.getUsername() + "\n");
		System.out.println("\t'" + msg + "'");
		return System.currentTimeMillis();
	}
}
