
package kvsnode;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import util.Log;
import util.LogWriter;

/**
 * This class is to check that the new virtual nodes simulated by the caller's
 * predecessor has been bounded. It checks the new virtual node id which is
 * supposed to be simulated at last. After it confirms the new virtual nodes are
 * bound, it executes some computation to remove the old virtual nodes or actual
 * node has been simulating the virtual nodes.
 * 
 * @author sky
 * @version 6/5/2013
 * 
 */
public class CheckerForNodeRebound implements Runnable
{

  /**
   * Checking interval.
   */
  public static final int CHECKING_INTERVAL = 2500;

  /**
   * A KVS node running this Runnable on.
   */
  private final KVSNode my_node;

  /**
   * A rebounding checking case.
   */
  private final ReboundCheckingCase my_case;

  /**
   * A node ID to check.
   */
  private final int my_checking_node_id;

  /**
   * An index of a node in my_virtual_node_ids (Used by join process).
   */
  private final int my_node_index_to_remove;

  /**
   * A predecessor actual node id of the caller actual node of this Runnable.
   * Used to log in leaving case.
   */
  private final int my_pre_node_id;

  /**
   * A constructor.
   * 
   * @param the_node a KVS node running this Runnable on.
   * @param the_case a rebounding checking case.
   * @param the_checking_node_id a node ID to check.
   * @param the_pre_node_id a predecessor actual node id of the caller actual
   *          node of this Runnable.
   * @param the_node_index_to_remove an index of a node in my_virtual_node_ids.
   */
  public CheckerForNodeRebound(final KVSNode the_node, final ReboundCheckingCase the_case,
                               final int the_checking_node_id, final int the_pre_node_id,
                               final int the_node_index_to_remove)
  {
    my_node = the_node;
    my_case = the_case;
    my_node_index_to_remove = the_node_index_to_remove;
    my_pre_node_id = the_pre_node_id;
    my_checking_node_id = the_checking_node_id;
  }

  /**
   * This method is to check that the new virtual nodes simulated by the
   * caller's predecessor has been bounded. It checks the new virtual node id
   * which is supposed to be simulated at last.
   */
  @Override
  public void run()
  {
    try
    {
      KVSNode node =
          (KVSNode) AbstractKVSNode.getRegistry().lookup(String.valueOf(my_checking_node_id));
      while (!node.isActive())
      {
        Log.out("Waiting for a new virtual node " + my_checking_node_id + " up.");
        // keep checking whether the new node is bound or not.
        Thread.sleep(CHECKING_INTERVAL);
        node =
            (KVSNode) AbstractKVSNode.getRegistry().
              lookup(String.valueOf(my_checking_node_id));
      }
      Log.out("The new virtual node " + my_checking_node_id + " has just been up.");
      if (my_case.equals(ReboundCheckingCase.JOIN))
      {
        ((ActualKVSNodeImpl) my_node).removeVirtualNodesForJoin(my_node_index_to_remove);
      }
      else
      {
        ((ActualKVSNodeImpl) my_node).sendOutToInformSuccessorUpdate();
        ((ActualKVSNodeImpl) my_node).removeAllNode();
        LogWriter.logLeaving(((AbstractKVSNode) my_node).getNodeId(), my_pre_node_id);
        Log.out("Bye bye ~ :)");
        System.exit(1);
      }
    }
    catch (final RemoteException | NotBoundException e1)
    {
      e1.printStackTrace();
    }
    catch (final InterruptedException e2)
    {
   // Do nothing.
    }
  }

}
