/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sky Moon
 */

package kvsnode;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * The implementation of KVSNode for virtual nodes.
 * 
 * @author sky
 * @version 5/9/2013
 */
public class VirtualKVSNodeImpl extends AbstractKVSNode
{

  /**
   * This is needed to deal with join(String) method invocation on the virtual
   * node. If the join method is invoked, it should let the actual node deal
   * with the situation.
   */
  private final KVSNode my_actual_node;

  /**
   * Construct the virtual node.
   * 
   * @param the_properties the property object having configuration information.
   * @param the_node_id the node id for this node.
   * @param the_actual_node an actual node simulating this node.
   */
  public VirtualKVSNodeImpl(final Properties the_properties, final int the_node_id,
                            final KVSNode the_actual_node)
  {
    super(the_properties);
    setActualNodeId(((AbstractKVSNode) the_actual_node).getNodeId());
    setNodeId(the_node_id);
    my_actual_node = the_actual_node;
    initializeQueues();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Collection<String>> joinToRing() throws RemoteException
  {
    final Map<String, Collection<String>> result =
        ((ActualKVSNodeImpl) my_actual_node).getDataForJoin(getNodeId());
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String invokeNodeToLeave() throws RemoteException
  {
    return "This node is a virtual node, cannot leave.";
  }

  /**
   * Return an actual node of this virtual node.
   * 
   * @return an actual node for the virtual node.
   */
  public KVSNode getActualNode()
  {
    return my_actual_node;
  }

  @Override
  public String getFirstKey() throws RemoteException
  {
    return my_actual_node.getFirstKey();
  }

  @Override
  public String getLastKey() throws RemoteException
  {
    return my_actual_node.getLastKey();
  }

  @Override
  public int getTotalNumberOfKeys() throws RemoteException
  {
    return my_actual_node.getTotalNumberOfKeys();
  }

}
