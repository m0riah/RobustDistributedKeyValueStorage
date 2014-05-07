
package kvsclient;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import kvsnode.KVSNode;

/**
 * To test remove a node.
 * 
 * @author sky
 * @version 5/18/2013
 */
public class KVSClientRemoveNodeTest
{
  /**
   * The name of Remote Registry (fake) inside the real registry.
   */
  public static final String REMOTE_REGISTRY = "RemoteRegistry";

  /**
   * Remove a node.
   * 
   * @param the_strings none.
   * @throws RemoteException any remote exception.
   * @throws NotBoundException any remote exception.
   */
  public static void main(final String... the_strings) throws RemoteException,
      NotBoundException
  {
    // final Registry registry = LocateRegistry.getRegistry("172.28.244.81",
    // 8005);
    final Registry registry = LocateRegistry.getRegistry("localhost", 8005);
    final Registry rr = (Registry) registry.lookup(REMOTE_REGISTRY);
    System.out.println("It's been connected to the RMI Registry");
    final KVSNode node = (KVSNode) rr.lookup("0");
    System.out.println(node.invokeNodeToLeave());
  }
}
