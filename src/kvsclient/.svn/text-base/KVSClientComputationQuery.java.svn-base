
package kvsclient;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;
import java.util.List;

import kvsnode.KVSNode;

/**
 * Client test class for the computation part (snapshot).
 * 
 * @author sky
 * @version 6/6/2013
 */
public class KVSClientComputationQuery
{
  /**
   * The name of Remote Registry (fake) inside the real registry.
   */
  public static final String REMOTE_REGISTRY = "RemoteRegistry";

  /**
   * Test a batch store queries.
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
    final KVSNode node = (KVSNode) rr.lookup(String.valueOf(0));
    System.out.println("First Key is: " +node.getFirstKey());
    System.out.println("Last Key is: " +node.getLastKey());
    System.out.println("Total Number of keys: " +
                        node.getTotalNumberOfKeys());
    final List<String> test = node.getActualNodeList();
    final Iterator<String> itr = test.iterator();
    while (itr.hasNext())
    {
      System.out.println("Actual nodes are: " +itr.next());
    }
    
  }
}
