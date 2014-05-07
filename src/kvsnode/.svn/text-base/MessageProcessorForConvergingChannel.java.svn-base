
package kvsnode;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import util.Log;
import util.LogWriter;

/**
 * This is a message processor thread in the converging queue (TransferQueue).
 * 
 * @author sky
 * @version 5/9/2013
 */
public class MessageProcessorForConvergingChannel implements Runnable
{

  /**
   * The checking interval to check the my_message_process_flag in a node.
   */
  public static final int CHECKING_INTERVAL = 1500;

  /**
   * The maximum wait time if it detects any node failure while sending out
   * messages. It should not be longer than KVSNode.CLIENT_QUERY_WAIT_TIME.
   */
  public static final int MAX_WAIT = KVSNode.CLIENT_QUERY_WAIT_TIME;

  /**
   * The data structure in a node.
   */
  private final RemoteMessage my_message;

  /**
   * KVSNode processing this thread.
   */
  private final KVSNode my_node;

  /**
   * A port id indicating where the message was from.
   */
  private final int my_port_id;

  /**
   * Constructor to process all the messages coming into all the channels.
   * 
   * @param the_message a message from other nodes.
   * @param the_port_id a port id.
   * @param the_node a node processing this thread.
   */
  public MessageProcessorForConvergingChannel(final RemoteMessage the_message,
                                              final int the_port_id, final KVSNode the_node)
  {
    my_message = the_message;
    my_port_id = the_port_id;
    my_node = the_node;
  }

  /**
   * Process a message one by one from the queue. If the final destination of
   * the message is the same as the current node id, then process the client
   * query inside the RemoteMessage object, then send it back to the original
   * source of the request message. If not, pass this object to another node
   * which is the intermediate location to get the final destination, or the
   * final destination.
   */
  @Override
  public void run()
  {
    /*
     * First, check whether the node is processing message. If it is false, some
     * kind of unstable situation in the ring happened, so the system is making
     * the ring system stable.
     */
    if (((AbstractKVSNode) my_node).isMessageProcessing())
    {
      // do nothing.
    }
    else
    {
      while (!((AbstractKVSNode) my_node).isMessageProcessing())
      {
        try
        {
          wait(CHECKING_INTERVAL);
        }
        catch (final InterruptedException e)
        {
          // Do nothing.
        }
      }
    }
    final int current_node_id = ((AbstractKVSNode) my_node).getNodeId();
    LogWriter.logMsgReceived(((AbstractKVSNode) my_node).getActualNodeId(), current_node_id,
                             my_port_id, my_message.getQueryType(), my_message.getMessage());

    if ((my_message.getQueryType().equals(QueryType.FIRST_KEY) ||
         my_message.getQueryType().equals(QueryType.LAST_KEY) ||
         my_message.getQueryType().equals(QueryType.TOTAL_KEY) || my_message.getQueryType()
        .equals(QueryType.ACTUAL_NODE)) &&
        (current_node_id == my_message.getFinalDestinationNode()))
    {
      processComputationQueries(current_node_id);
    }
    else
    {
      if (my_message.getFinalDestinationNode() == current_node_id &&
          my_message.isReturnValue())
      {
        processResultMessage();
      }
      // If a final destination of the message is equal to the node id, the
      // message is a query message and the node is connected to the RMI
      // registry.
      else if (my_message.getFinalDestinationNode() == current_node_id &&
               !my_message.isReturnValue() && ((AbstractKVSNode) my_node).isActive())
      {
        processQueryMessage(current_node_id);
      }
      // If a final destination of the message is not equal to the node id.
      else
      {
        passMessageAgain(current_node_id);
      }
    }
  }

  /**
   * This method processes computation queries.
   * 
   * @param the_current_node_id a current node running this thread.
   */
  private void processComputationQueries(final int the_current_node_id)
  {
    if ((my_message.getOriginalDepartureNode() == the_current_node_id) &&
        my_message.isReturnValue())
    {
      if (my_message.getQueryType().equals(QueryType.ACTUAL_NODE))
      {
        final Map<String, Collection<String>> anode_data =
            (HashMap<String, Collection<String>>) my_message.getDataset();
        final List<String> anode_list = (LinkedList<String>) anode_data.get("anode_list");
        final Result result =
            ((AbstractKVSNode) my_node).getSentMessageTable().get(my_message.getMessageId());
        result.setResult(anode_list);
        result.setResultFlag(true);
        // wake up the sleeping thread in the storeData or retrieveData in
        // AbstractKVSNode.
        result.getThread().interrupt();
      }
      else
      {
        processResultMessage();
      }
    }
    else
    {
      if (my_message.getQueryType().equals(QueryType.FIRST_KEY))
      {
        ((ActualKVSNodeImpl) my_node)
            .calculateFirstKeyThenSendOut(my_message.getMessage(), my_message.getMessageId(),
                                          my_message.getOriginalDepartureNode());
      }
      else if (my_message.getQueryType().equals(QueryType.LAST_KEY))
      {
        ((ActualKVSNodeImpl) my_node)
            .calculateLastKeyThenSendOut(my_message.getMessage(), my_message.getMessageId(),
                                         my_message.getOriginalDepartureNode());
      }
      else if (my_message.getQueryType().equals(QueryType.TOTAL_KEY))
      {
        ((ActualKVSNodeImpl) my_node).calculateTotalNumberOfKeysThenSendOut(my_message
            .getMessage(), my_message.getMessageId(), my_message.getOriginalDepartureNode());
      }
      else if (my_message.getQueryType().equals(QueryType.ACTUAL_NODE))
      {
        final Map<String, Collection<String>> anode_data =
            (HashMap<String, Collection<String>>) my_message.getDataset();
        final List<String> anode_list = (LinkedList<String>) anode_data.get("anode_list");
        ((AbstractKVSNode) my_node).askNodeIdentityThenSendOut(anode_list, my_message
            .getMessageId(), my_message.getOriginalDepartureNode());
      }
    }
  }

  /**
   * Process a result from other node which was originally sent out from this
   * node.
   */
  private void processResultMessage()
  {
    final Result result =
        ((AbstractKVSNode) my_node).getSentMessageTable().get(my_message.getMessageId());
    result.setResult(my_message.getMessage());
    result.setResultFlag(true);
    // wake up the sleeping thread in the storeData or retrieveData in
    // AbstractKVSNode.
    result.getThread().interrupt();
  }

  /**
   * Process a query message which should be dealt in this node.
   * 
   * @param the_current_node_id a node id performing this thread.
   */
  private void processQueryMessage(final int the_current_node_id)
  {
    if (my_message.getQueryType().equals(QueryType.UPDATE_SUCCESSORS))
    {
      // update its successor actual nodes.
      ((ActualKVSNodeImpl) my_node).findSuccessors(the_current_node_id, 0);
    }
    else if (my_message.getQueryType().equals(QueryType.BACKUP))
    {
      processBackupQuery(the_current_node_id);
    }
    else if (my_message.getQueryType().equals(QueryType.RETRIEVE))
    {
      processRetrieveQuery(the_current_node_id);
    }
    else if (my_message.getQueryType().equals(QueryType.STORE))
    {
      processStoreQuery(the_current_node_id);
    }
    else if (my_message.getQueryType().equals(QueryType.VNODES_DATA))
    {
      ((ActualKVSNodeImpl) my_node).simulateVNodeWithData(my_message.getDataset());
    }
    else if (my_message.getQueryType().equals(QueryType.RESTORE))
    {
      ((ActualKVSNodeImpl) my_node).restoreFailureNodes();
    }
    else if (my_message.getQueryType().equals(QueryType.BACKUP_ALL_IN_PRED))
    {
      ((ActualKVSNodeImpl) my_node).updateBackupData(my_message.getDataset());
    }
    else if (my_message.getQueryType().equals(QueryType.BACKUP_ALL_IN_SUCC))
    {
      ((ActualKVSNodeImpl) my_node).updateBackupDataFromSucToPre(my_message.getDataset());
    }
    else if (my_message.getQueryType().equals(QueryType.BACKUP_REQUEST))
    {
      ((ActualKVSNodeImpl) my_node).sendBackupData(my_message.getOriginalDepartureNode());
    }
    else
    {
      Log.err("Uncategorized query type");
    }

  }

  /**
   * To process Backup query. Check whether the current node is an actual node,
   * then store the message to the backup data set in the actual node. Then send
   * back the result to the original departure node with the result.
   * 
   * @param the_current_node_id a node id performing this thread.
   */
  private void processBackupQuery(final int the_current_node_id)
  {
    /*
     * If this node is an actual node, store the message (a client query key)
     * into the backup storage in the node. If not, pass it to its actual node
     * to store the message. Then make it as a returning message with returning
     * message and send it to the original destination.
     */
    if (the_current_node_id == ((AbstractKVSNode) my_node).getActualNodeId())
    {
      final Set<String> dataset =
          ((ActualKVSNodeImpl) my_node).getBackupData().get(my_message
                                                                .getBackupRequestingNode());
      // if there is only one actual node in the ring, discard the backup
      // message.
      if (!((ActualKVSNodeImpl) my_node).getSuccessors().isEmpty())
      {
        dataset.add(my_message.getMessage());
      }
    }
    else
    {
      final KVSNode actual_node =
          (ActualKVSNodeImpl) ((VirtualKVSNodeImpl) my_node).getActualNode();
      final Set<String> dataset =
          ((ActualKVSNodeImpl) actual_node).getBackupData()
              .get(my_message.getBackupRequestingNode());
      // if there is only one actual node in the ring, discard the backup
      // message.
      if (!((ActualKVSNodeImpl) actual_node).getSuccessors().isEmpty())
      {
        dataset.add(my_message.getMessage());
      }
    }
    LogWriter.logValueBackedUp(((AbstractKVSNode) my_node).getActualNodeId(),
                               ((AbstractKVSNode) my_node).getActualNodeId(),
                               my_message.getMessage(), "SUCCESSFUL");
    final RemoteMessage return_message =
        new RemoteMessage(my_message.getBackupRequestingNode(), -1,
                          my_message.getOriginalDepartureNode(), true, QueryType.STORE,
                          my_message.getMessageId(), "STORED", null);
    final int next_hop =
        ((AbstractKVSNode) my_node).getRouteTracker()
            .getNextHop(return_message.getFinalDestinationNode(), the_current_node_id);
    final long time = System.currentTimeMillis();
    boolean failure = true;
    while (failure && System.currentTimeMillis() - time < MAX_WAIT)
    {
      try
      {
        final KVSNode next_node =
            (KVSNode) AbstractKVSNode.getRegistry().lookup(String.valueOf(next_hop));
        next_node.sendMessage(the_current_node_id, the_current_node_id, return_message);
        LogWriter.logMsgSent(((AbstractKVSNode) my_node).getActualNodeId(),
                             the_current_node_id, next_hop, return_message.getQueryType(),
                             return_message.getMessage());
        failure = false;
      }
      catch (final RemoteException | NotBoundException e)
      {
        LogWriter.logSystemFailure(((AbstractKVSNode) my_node).getActualNodeId(),
                                   the_current_node_id, next_hop, return_message.getMessage());
        // Failure detected. Notify the failure to its actual node.
        ((AbstractKVSNode) my_node).notifyNodeFailure(next_hop);
      }
    }
  }

  /**
   * To process Retrieve query. Check whether the string is in the dataset, then
   * send back the result to the original departure node with the result.
   * 
   * @param the_current_node_id a node id performing this thread.
   */
  private void processRetrieveQuery(final int the_current_node_id)
  {
    String result;
    // if the database contains the queried word, then returns "OK", unless "NO"
    if (((AbstractKVSNode) my_node).getSynchronizedSet().contains(my_message.getMessage()))
    {
      result = "OK";
    }
    else
    {
      result = "NO";
    }
    LogWriter.logValuesRet(((AbstractKVSNode) my_node).getActualNodeId(), the_current_node_id,
                           my_message.getMessage(), result);
    final RemoteMessage return_message =
        new RemoteMessage(the_current_node_id, -1, my_message.getOriginalDepartureNode(),
                          true, my_message.getQueryType(), my_message.getMessageId(), result,
                          null);
    final int next_hop =
        ((AbstractKVSNode) my_node).getRouteTracker()
            .getNextHop(return_message.getFinalDestinationNode(), the_current_node_id);
    final long time = System.currentTimeMillis();
    boolean failure = true;
    while (failure && System.currentTimeMillis() - time < MAX_WAIT)
    {
      try
      {
        final KVSNode next_node =
            (KVSNode) AbstractKVSNode.getRegistry().lookup(String.valueOf(next_hop));

        next_node.sendMessage(the_current_node_id, the_current_node_id, return_message);
        LogWriter.logMsgSent(((AbstractKVSNode) my_node).getActualNodeId(),
                             the_current_node_id, next_hop, return_message.getQueryType(),
                             return_message.getMessage());
        failure = false;
      }
      catch (final RemoteException | NotBoundException e)
      {
        LogWriter.logSystemFailure(((AbstractKVSNode) my_node).getActualNodeId(),
                                   the_current_node_id, next_hop, return_message.getMessage());
        // Failure detected. notify the failure to its actual node.
        ((AbstractKVSNode) my_node).notifyNodeFailure(next_hop);
      }
    }
  }

  /**
   * To process Store query. If the message is successfully stored in the
   * dataset, pass the message to a node (actual node of this node - 1). The
   * message will be treated as a backup message regardless of the receiver node
   * is actual or virtual, and be stored in the backup map in the first
   * predecessor actual node. Else, it just returns fail message to its original
   * departure the client is waiting on.
   * 
   * @param the_current_node_id a node id performing this thread.
   */
  private void processStoreQuery(final int the_current_node_id)
  {
    RemoteMessage return_message;
    String result;
    /*
     * Pass the message to a node (actual node of this node - 1). The message
     * will be treated as a backup message regardless of the receiver node is
     * actual or virtual, and be stored in the backup map in the first
     * predecessor actual node.
     */
    ((AbstractKVSNode) my_node).getSynchronizedSet().add(my_message.getMessage());
    result = "STORED";
    final int actual_node = ((AbstractKVSNode) my_node).getActualNodeId();
    final int backup_dest =
        ((AbstractKVSNode) my_node).getRouteTracker().getPrevNodeInRing(actual_node);
    return_message =
        new RemoteMessage(my_message.getOriginalDepartureNode(), the_current_node_id,
                          backup_dest, false, QueryType.BACKUP, my_message.getMessageId(),
                          my_message.getMessage(), null);
    LogWriter.logValueStored(((AbstractKVSNode) my_node).getActualNodeId(),
                             the_current_node_id, my_message.getMessage(), result);
    final int next_hop =
        ((AbstractKVSNode) my_node).getRouteTracker()
            .getNextHop(return_message.getFinalDestinationNode(), the_current_node_id);
    final long time = System.currentTimeMillis();
    boolean failure = true;
    while (failure && System.currentTimeMillis() - time < MAX_WAIT)
    {
      try
      {
        final KVSNode next_node =
            (KVSNode) AbstractKVSNode.getRegistry().lookup(String.valueOf(next_hop));
        next_node.sendMessage(the_current_node_id, the_current_node_id, return_message);
        LogWriter.logMsgSent(((AbstractKVSNode) my_node).getActualNodeId(),
                             the_current_node_id, next_hop, return_message.getQueryType(),
                             return_message.getMessage());
        failure = false;
      }
      catch (final RemoteException | NotBoundException e)
      {
        // Failure detected. Notify the failure to its actual node.
        LogWriter.logSystemFailure(((AbstractKVSNode) my_node).getActualNodeId(),
                                   the_current_node_id, next_hop, return_message.getMessage());
        ((AbstractKVSNode) my_node).notifyNodeFailure(next_hop);
      }
    }
  }

  /**
   * Pass the message to other node taking the responsibility.
   * 
   * @param the_current_node_id a node id performing this thread.
   */
  private void passMessageAgain(final int the_current_node_id)
  {
    final int next_hop =
        ((AbstractKVSNode) my_node).getRouteTracker()
            .getNextHop(my_message.getFinalDestinationNode(), the_current_node_id);
    final long time = System.currentTimeMillis();
    boolean failure = true;
    while (failure && System.currentTimeMillis() - time < MAX_WAIT)
    {
      try
      {
        // send the message again to another node which might take care of the
        // message.
        final KVSNode next_node =
            (KVSNode) AbstractKVSNode.getRegistry().lookup(String.valueOf(next_hop));
        next_node.sendMessage(the_current_node_id, the_current_node_id, my_message);
        LogWriter.logMsgSent(((AbstractKVSNode) my_node).getActualNodeId(),
                             the_current_node_id, next_hop, my_message.getQueryType(),
                             my_message.getMessage());
        failure = false;
      }
      catch (final RemoteException | NotBoundException e)
      {
        // Failure detected. Notify the failure to its actual node.
        LogWriter.logSystemFailure(((AbstractKVSNode) my_node).getActualNodeId(),
                                   the_current_node_id, next_hop, my_message.getMessage());
        ((AbstractKVSNode) my_node).notifyNodeFailure(next_hop);
      }
    }
  }

}
