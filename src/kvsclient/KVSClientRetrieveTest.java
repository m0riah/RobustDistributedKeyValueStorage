
package kvsclient;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import kvsnode.KVSNode;
import util.Hasher;
import util.HasherImpl;
import util.Log;

/**
 * To test batch retrieve queries.
 * 
 * @author sky
 * @version 5/18/2013
 */
public class KVSClientRetrieveTest
{
  /**
   * The name of Remote Registry (fake) inside the real registry.
   */
  public static final String REMOTE_REGISTRY = "RemoteRegistry";

  /**
   * Testing batch(100) Retrieve query.
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
    final Hasher hasher = new HasherImpl(32);
    for (int i = 10; i < 256; i++)
    {
      final KVSNode node = (KVSNode) rr.lookup(String.valueOf(i));
      Log.out("Test" + i + " Hashvalue " + hasher.getValue("Test" + i));
      Log.out("Node: " + hasher.getValue("Test" + i));
      final String retrieve = (String) node.retrieveData("Test" + i);
      Log.out("Test" + i + ", Result : " + retrieve);
    }
  }
}
