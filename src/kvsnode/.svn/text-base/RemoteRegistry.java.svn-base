/*
 * Spring 2013 TCSS558 - Applied Distributed Computing
 * Java RMI Remote Registry
 * Daniel M. Zimmerman
 */

package kvsnode;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An RMI registry that can be used by remote Java VMs, but requires
 * bootstrapping by being looked up from a "real" RMI registry.
 * 
 * @author Daniel M. Zimmerman
 * @version May 2013
 */
public class RemoteRegistry implements Registry
{
  /**
   * The default proxy registry name.
   */
  public static final String DEFAULT_NAME = "RemoteRegistry";
  
  /**
   * The minimum TCP port on which to create a registry.
   */
  public static final int MIN_PORT = 1025;
  
  /**
   * The maximum TCP port on which to create a registry.
   */
  public static final int MAX_PORT = 65535;
  
  /**
   * The remote registry.
   */
  private static RemoteRegistry static_registry;
  
  /**
   * The Map in which to store the bindings.
   */
  private final Map<String, Remote> my_bindings;

  /**
   * Constructs a new ProxyRegistry object for the specified RMI
   * registry.
   */
  public RemoteRegistry()
  {
    my_bindings = Collections.synchronizedMap(new HashMap<String, Remote>());
  }
  
  @Override
  public Remote lookup(final String the_name) 
    throws RemoteException, NotBoundException, AccessException
  {
    return my_bindings.get(the_name);
  }

  @Override
  public void bind(final String the_name, final Remote the_object) 
    throws RemoteException, AlreadyBoundException, AccessException
  {
    synchronized (my_bindings)
    {
      if (my_bindings.get(the_name) != null)
      {
        throw new AlreadyBoundException(the_name + " is already bound to " + 
                                        my_bindings.get(the_name));
      }
      my_bindings.put(the_name, the_object);
    }
  }

  @Override
  public void unbind(final String the_name) 
    throws RemoteException, NotBoundException, AccessException
  {
    synchronized (my_bindings)
    {
      if (my_bindings.get(the_name) == null)
      {
        throw new NotBoundException(the_name + " is not bound");
      }
      my_bindings.remove(the_name);
    }
  }

  @Override
  public void rebind(final String the_name, final Remote the_object) 
    throws RemoteException, AccessException
  {
    my_bindings.put(the_name, the_object);
  }

  @Override
  public String[] list() throws RemoteException, AccessException
  {
    synchronized (my_bindings)
    {
      return my_bindings.keySet().toArray(new String[0]); 
    }
  }

  /**
   * The main method. Starts an RMI registry on the specified port, then 
   * registers this remote registry with either the specified
   * name or the default name "RemoteRegistry".
   * 
   * @param the_args The command line arguments. The first is always the 
   * port number on which to start the real RMI registry; the second, if
   * it is passed, is the name with which to bind the remote registry. 
   * The default name is "RemoteRegistry". 
   */
  public static void main(final String... the_args)
  {
    try
    {
      final int port = Integer.parseInt(the_args[0]);
      String name = DEFAULT_NAME;
      
      if (port < MIN_PORT || MAX_PORT < port)
      {
        System.err.println("port must be between 1025 and 65535 inclusive");
        System.exit(1);
      }
      if (the_args.length > 1)
      {
        name = the_args[1];
      }
      
      final Registry registry = LocateRegistry.createRegistry(port);
      static_registry = new RemoteRegistry();
      registry.bind(name, UnicastRemoteObject.exportObject(static_registry, 0));
      System.out.println("RMI registry listening on port " + port);
      System.out.println("Remote registry bound with name '" + name + "'");
    }
    catch (final AccessException e)
    {
      System.err.println("issue accessing registry: " + e);
      System.exit(1);
    }
    catch (final AlreadyBoundException e)
    {
      System.err.println("already bound???: " + e);
      System.exit(1);
    }
    catch (final RemoteException e)
    {
      System.err.println("a problem occurred: " + e);
      System.exit(1);
    }
    catch (final ArrayIndexOutOfBoundsException | NumberFormatException e)
    {
      System.err.println("Usage: java -jar remoteregistry.jar <port> [name]");
    }
  }
}
