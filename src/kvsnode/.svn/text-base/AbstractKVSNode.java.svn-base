/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sky Moon
 */

package kvsnode;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import util.Hasher;
import util.HasherImpl;
import util.Log;
import util.LogWriter;
import util.RouteTracker;
import util.RouteTrackerImpl;

/**
 * This abstract class is to reduce the redundancy of the KVSNodes.
 * 
 * @author Sky Moon
 * @version 5/4/2013
 */
public abstract class AbstractKVSNode implements KVSNode
{

  /**
   * The internal(fake) RMI Remote registry.
   */
  private static Registry static_registry;

  /**
   * If it is true, then keeps processing the messages. If not, stop processing
   * messages.
   */
  private boolean my_message_process_flag;

  /**
   * If this node is connected to rmi registry, then true. If not, false. This
   * is needed to MessageProcessorForConvergingChannel.java. If the flag is
   * dead, and the non-returning message should be processed in this node, then
   * just pass it again to the same node id in the rmi registry which is
   * connected (alive).
   */
  private boolean my_active_token;

  /**
   * This field is just to send non synchronized set to other place. Only reason
   * we are doing that is that we are not sure that
   * Collections.synchronizedSortedSet is maintained after it is unserialized
   * then serialized again.
   * 
   * Java7 API says about Collections.synchronizedSortedSet that "The returned
   * sorted set will be serializable if the specified sorted set is
   * serializable, but still not sure whether the function(synchronized) is
   * working. Rather test it, we decide just sending core TreeSet and wrap it
   * with Collections.synchronizedSortedSet at the recieving node side.
   */
  private SortedSet<String> my_core_dataset;

  /**
   * The TreeSet containing the word list. We chose a TreeSet because it sorts
   * the entries and allows for relatively quick searching. This is
   * synchronized.
   */
  private SortedSet<String> my_dataset;

  /**
   * A map of multiple blocking queues to implement message channels (port id,
   * blocking queue).
   */
  private final Map<String, ExecutorService> my_port_queues =
      new HashMap<String, ExecutorService>();

  /**
   * A map to keep track the sent message with relevant Future object which was
   * sent to the clients.
   */
  private final Map<Integer, Result> my_sent_message_table =
      new ConcurrentHashMap<Integer, Result>();

  /**
   * This is a media which connects blocking queues to one thread.
   */
  private ExecutorService my_transfer_queue;

  /**
   * To hash a string input to get the key for the string to find the
   * appropriate node.
   */
  private final Hasher my_hasher;

  /**
   * To find the quickest next node to send the string to the appropriate node.
   */
  private final RouteTracker my_route_tracker;

  /**
   * A default environment setting.
   * 
   * - related instruction in the instruction document -
   * 
   * This means that you should not design or implement your system in ways that
   * will unnecessarily constrain you later. Examples of such unnecessary
   * constraints would be hard-coding the locations of data files, IP addresses,
   * numbers of nodes in your system, etc.Use the command line or configuration
   * files
   */
  private final Properties my_properties;

  /**
   * Total number of nodes in the ring. This information is to reduce the
   * calculation to get the total number of nodes from the value m.
   */
  private final int my_total_node_number;

  /**
   * A node id for the node.
   */
  private int my_node_id;

  /**
   * A actual node id for the node.
   */
  private int my_actual_node_id;

  /**
   * Constructor for the AbstractKVSNode.
   * 
   * @param the_properties a property file for the node.
   */
  public AbstractKVSNode(final Properties the_properties)
  {
    my_properties = the_properties;
    my_core_dataset = new TreeSet<String>();
    my_dataset = Collections.synchronizedSortedSet(my_core_dataset);
    my_node_id = Integer.valueOf(the_properties.getProperty("nodeid"));
    my_message_process_flag = true;
    my_active_token = true;
    my_total_node_number = (int) Math.pow(2, Integer.valueOf(the_properties.getProperty("m")));
    my_hasher = new HasherImpl(my_total_node_number);
    my_route_tracker = new RouteTrackerImpl(Integer.valueOf(the_properties.getProperty("m")));
    initializeQueues();
  }

  /**
   * Initialize the blocking queues for each port id, and a transfer queue for a
   * node. The number of blocking queues is equal to the value of m. This should
   * be performed after creating the node to initialize the port ids. It cannot
   * be performed because of this limitation. We are not sure whether a created
   * node can take the node id it is supposed to take until it communicates with
   * the RMI and makes sure that there is no node having the same node id.
   * 
   */
  public void initializeQueues()
  {
    final int m = Integer.valueOf(my_properties.getProperty("m"));
    BlockingQueue<Runnable> queue =
        new LinkedBlockingQueue<Runnable>(QUEUE_LENGTH_FOR_CHANNELS);
    my_port_queues.put(String.valueOf(getNodeId()),
                       new ThreadPoolExecutor(POOL_SIZE_FOR_PORT_QUEUE,
                                              POOL_SIZE_FOR_PORT_QUEUE,
                                              ALIVE_SEC_OF_IDLE_THREAD, TimeUnit.SECONDS,
                                              queue));
    for (int i = 0; i < m; i++)
    {
      queue = new LinkedBlockingQueue<Runnable>(QUEUE_LENGTH_FOR_CHANNELS);
      final String port_id =
          String
              .valueOf(my_route_tracker.findIncomingChannel(my_node_id, (int) Math.pow(2, i)));
      // Use modular arithmetic instead if/else condition statement
      // each thread pool has a thread which grab a message in each channel and
      // put it into the TransferQueue, and a blocking queue.
      my_port_queues.put(port_id, new ThreadPoolExecutor(POOL_SIZE_FOR_PORT_QUEUE,
                                                         POOL_SIZE_FOR_PORT_QUEUE,
                                                         ALIVE_SEC_OF_IDLE_THREAD,
                                                         TimeUnit.SECONDS, queue));
    }
    final TransferQueue<Runnable> t_queue = new LinkedTransferQueue<Runnable>();
    my_transfer_queue =
        new ThreadPoolExecutor(POOL_SIZE_FOR_PORT_QUEUE, POOL_SIZE_FOR_PORT_QUEUE,
                               ALIVE_SEC_OF_IDLE_THREAD, TimeUnit.SECONDS, t_queue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String retrieveData(final String the_message) throws RemoteException
  {
    LogWriter.logClientRetrieveData(my_actual_node_id, my_node_id, the_message);
    String result = REQUEST_FAIL;
    final int hash_value = my_hasher.getValue(the_message);
    final int message_id = getMessageId();
    final RemoteMessage message =
        new RemoteMessage(my_node_id, -1, hash_value, false, QueryType.RETRIEVE, message_id,
                          the_message, null);
    my_transfer_queue.submit(new MessageProcessorForConvergingChannel(message, getNodeId(),
                                                                      this));
    my_sent_message_table.put(message_id, new Result(Thread.currentThread()));
    final long time = System.currentTimeMillis();
    while (!my_sent_message_table.get(message_id).isResultArrived() &&
           System.currentTimeMillis() - time < CLIENT_QUERY_WAIT_TIME)
    {
      try
      {
        Thread.sleep(CLIENT_QUERY_WAIT_TIME);
      }
      catch (final InterruptedException e)
      {

      }
    }
    if (my_sent_message_table.get(message_id).isResultArrived())
    {
      result = (String) my_sent_message_table.get(message_id).getResult();
      my_sent_message_table.get(message_id).setResultPassed(true);
    }
    LogWriter.logValuesRet(my_actual_node_id, my_node_id, the_message, result);
    return result;
  }

  /**
   * {@inheritDoc}
   */
  public String storeData(final String the_message) throws RemoteException
  {
    LogWriter.logClientStoreData(my_actual_node_id, my_node_id, the_message);
    String result = REQUEST_FAIL;
    final int hash_value = my_hasher.getValue(the_message);
    final int message_id = getMessageId();
    final RemoteMessage message =
        new RemoteMessage(my_node_id, -1, hash_value, false, QueryType.STORE, message_id,
                          the_message, null);
    my_transfer_queue.submit(new MessageProcessorForConvergingChannel(message, getNodeId(),
                                                                      this));
    my_sent_message_table.put(message_id, new Result(Thread.currentThread()));
    final long time = System.currentTimeMillis();
    while (!my_sent_message_table.get(message_id).isResultArrived() &&
           System.currentTimeMillis() - time < CLIENT_QUERY_WAIT_TIME)
    {
      try
      {
        Thread.sleep(CLIENT_QUERY_WAIT_TIME);
      }
      catch (final InterruptedException e)
      {

      }
    }
    if (my_sent_message_table.get(message_id).isResultArrived())
    {
      result = (String) my_sent_message_table.get(message_id).getResult();
      my_sent_message_table.get(message_id).setResultPassed(true);
    }
    LogWriter.logValueStored(my_actual_node_id, my_node_id, the_message, result);
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void sendMessage(final int the_source_node_id, final int the_dest_port_id,
                          final RemoteMessage the_message) throws RemoteException
  {
    final String port_id = String.valueOf(the_dest_port_id);
    my_port_queues.get(port_id).submit(new MessageProcessorForEachChannel(my_transfer_queue,
                                                                          the_dest_port_id,
                                                                          the_message, this));
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<String> getActualNodeList() throws RemoteException
  {
    final int msg_id = getMessageId();
    getSentMessageTable().put(msg_id, new Result(Thread.currentThread()));
    askNodeIdentityThenSendOut(null, msg_id, getNodeId());
    final long time = System.currentTimeMillis();
    while (!getSentMessageTable().get(msg_id).isResultArrived() &&
           System.currentTimeMillis() - time < CLIENT_QUERY_WAIT_TIME)
    {
      try
      {
        Thread.sleep(CLIENT_QUERY_WAIT_TIME);
      }
      catch (final InterruptedException e)
      {

      }
    }
    final List<String> anode_list = new LinkedList<String>();
    if (getSentMessageTable().get(msg_id).isResultArrived())
    {
      final Result result = getSentMessageTable().get(msg_id);
      anode_list.addAll((LinkedList<String>) result.getResult());
      LogWriter.logListofActualNode(getNodeId(), anode_list);
      getSentMessageTable().get(msg_id).setResultPassed(true);
    }
    return anode_list;
  }

  /**
   * To find a list of actual nodes in the ring. This will use sequential
   * checking around the ring.
   * 
   * @param the_anode_list a list of nodes.
   * @param the_msg_id a message id.
   * @param the_final_dest a final destination of the snapshot.
   */
  public void askNodeIdentityThenSendOut(final List<String> the_anode_list,
                                         final int the_msg_id, final int the_final_dest)
  {
    final List<String> anode_list = new LinkedList<String>();
    if (the_anode_list != null)
    {
      anode_list.addAll(the_anode_list);
    }
    if (my_node_id == my_actual_node_id)
    {
      anode_list.add(String.valueOf(my_node_id));
    }
    final Map<String, Collection<String>> dataset = new HashMap<String, Collection<String>>();
    dataset.put("anode_list", anode_list);
    final int next_node_id = getRouteTracker().getNextNodeInRing(getNodeId());
    final RemoteMessage f_message =
        new RemoteMessage(the_final_dest, -1, next_node_id, true, QueryType.ACTUAL_NODE,
                          the_msg_id, null, dataset);
    final int next_hop = getRouteTracker().getNextHop(next_node_id, getNodeId());
    try
    {
      final KVSNode next_node = (KVSNode) getRegistry().lookup(String.valueOf(next_hop));
      next_node.sendMessage(getNodeId(), getNodeId(), f_message);
    }
    catch (final RemoteException | NotBoundException e1)
    {
      e1.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isActualNode() throws RemoteException
  {
    boolean result = false;
    if (my_node_id == my_actual_node_id)
    {
      result = true;
    }
    return result;
  }

  /**
   * Notify a node failure to its actual node.
   * 
   * @param the_failure_node_id a failure node id.
   * @return true if the process is successful, false else.
   */
  public boolean notifyNodeFailure(final int the_failure_node_id)
  {
    final KVSNode actual_node;
    if (my_node_id == my_actual_node_id)
    {
      actual_node = this;
    }
    else
    {
      actual_node = ((VirtualKVSNodeImpl) this).getActualNode();
    }
    final int responsible_node = getResponsibleNode(actual_node, the_failure_node_id);
    try
    {
      final RemoteMessage restore_message =
          new RemoteMessage(my_node_id, -1, responsible_node, false, QueryType.RESTORE, -1,
                            null, null);
      if (responsible_node == my_actual_node_id)
      {
        if (my_node_id == my_actual_node_id)
        {
          ((ActualKVSNodeImpl) this).restoreFailureNodes();
        }
        else
        {
          final KVSNode an = ((VirtualKVSNodeImpl) this).getActualNode();
          ((ActualKVSNodeImpl) an).restoreFailureNodes();
        }
      }
      else
      {
        final int next_hop = my_route_tracker.getNextHop(responsible_node, my_node_id);
        // restore system.
        final KVSNode next_node = (KVSNode) getRegistry().lookup(String.valueOf(next_hop));
        next_node.sendMessage(my_node_id, my_node_id, restore_message);
      }
    }
    catch (final RemoteException | NotBoundException e)
    {
      Log.err(e.toString());
    }
    return true;
  }

  /**
   * Calculate the responsible actual node to restore the system.
   * 
   * @param the_actual_node an actual node for this node.
   * @param the_failure_node_id a failure node id.
   * @return a responsible actual node to restore the system.
   */
  public int getResponsibleNode(final KVSNode the_actual_node, final int the_failure_node_id)
  {
    // Find a failure actual node.
    int max_anode_id = 0;
    int failure_anode_id = -1;
    Iterator<Integer> itr = ((ActualKVSNodeImpl) the_actual_node).getSuccessors().iterator();
    while (itr.hasNext())
    {
      final int anode_id = itr.next();
      if (max_anode_id < anode_id)
      {
        max_anode_id = anode_id;
      }
      if (the_failure_node_id >= anode_id && anode_id > failure_anode_id)
      {
        failure_anode_id = anode_id;
      }
    }
    if (failure_anode_id == -1)
    {
      failure_anode_id = max_anode_id;
    }
    // find direct predecessor of the failure actual node.
    itr = ((ActualKVSNodeImpl) the_actual_node).getSuccessors().iterator();
    int direct_pred_node = my_actual_node_id;
    int prenode = my_actual_node_id;
    while (itr.hasNext())
    {
      final int anode_id = itr.next();
      if (failure_anode_id == anode_id)
      {
        direct_pred_node = prenode;
        break;
      }
      else
      {
        prenode = anode_id;
      }
    }
    return direct_pred_node;
  }

  /**
   * A getter for registry.
   * 
   * @return RMI registry.
   */
  public static Registry getRegistry()
  {
    return static_registry;
  }

  /**
   * A setter for a registry.
   * 
   * @param the_static_registry set a registry.
   */
  public static void setRegistry(final Registry the_static_registry)
  {
    static_registry = the_static_registry;
  }

  /**
   * A getter for Properties.
   * 
   * @return a properties object.
   */
  public Properties getProperties()
  {
    return my_properties;
  }

  /**
   * A getter for node id.
   * 
   * @return a node id.
   */
  public int getNodeId()
  {
    return my_node_id;
  }

  /**
   * A setter for node id.
   * 
   * @param the_node_id a node id.
   */
  public void setNodeId(final int the_node_id)
  {
    my_node_id = the_node_id;
  }

  /**
   * A getter for actual node id.
   * 
   * @return an actual node id.
   */
  public int getActualNodeId()
  {
    return my_actual_node_id;
  }

  /**
   * A setter for actual node id.
   * 
   * @param the_actual_node_id a actual node id.
   */
  public void setActualNodeId(final int the_actual_node_id)
  {
    my_actual_node_id = the_actual_node_id;
  }

  /**
   * Returns a number of total number of the nodes in the ring.
   * 
   * @return the number of nodes in the ring.
   */
  public int getTotalNodeNumber()
  {
    return my_total_node_number;
  }

  /**
   * A Getter for a hasher.
   * 
   * @return a hasher for this node.
   */
  public Hasher getHasher()
  {
    return my_hasher;
  }

  /**
   * A Getter for a route tracker.
   * 
   * @return a RouteTracker.
   */
  public RouteTracker getRouteTracker()
  {
    return my_route_tracker;
  }

  /**
   * A getter for a sent message table.
   * 
   * @return a map represent a sent message table.
   */
  public Map<Integer, Result> getSentMessageTable()
  {
    return my_sent_message_table;
  }

  /**
   * It simply returns the number which is last message id + 1.
   * 
   * @return message id which is available.
   */
  public int getMessageId()
  {
    return my_sent_message_table.size();
  }

  /**
   * A getter for database.
   * 
   * @return a set representing a database.
   */
  public SortedSet<String> getCoreDataSet()
  {
    return my_core_dataset;
  }

  /**
   * A getter for sychronized database.
   * 
   * @return a set representing a database.
   */
  public Set<String> getSynchronizedSet()
  {
    return my_dataset;
  }

  /**
   * A setter for database.
   * 
   * @param the_dataset a database.
   */
  public void setCoreDataSet(final SortedSet<String> the_dataset)
  {
    my_core_dataset = the_dataset;
    my_dataset = Collections.synchronizedSortedSet(the_dataset);
  }

  /**
   * True, if the message keeps processing, false, if the message processing is
   * halt.
   * 
   * @return defined at the method definition.
   */
  public boolean isMessageProcessing()
  {
    return my_message_process_flag;
  }

  /**
   * Set the message process flag.
   * 
   * @param the_message_process_flag a message process flag.
   */
  public void setMessageProcessFlag(final boolean the_message_process_flag)
  {
    this.my_message_process_flag = the_message_process_flag;
  }

  /**
   * To check the TransferQueue is empty. This is supposed t be used for
   * checking whether there is still processing message before remove a node.
   * 
   * @return true if it is empty, false if it is not empty.
   */
  public boolean isTransferQueueEmpty()
  {
    boolean result = false;
    if (null == ((ThreadPoolExecutor) my_transfer_queue).getQueue().peek())
    {
      result = true;
    }
    return result;
  }

  /**
   * Return the alive token, my_alive.
   * 
   * @return true if this node is alive (has a connection to the RMI registry),
   *         false if not.
   */
  @Override
  public boolean isActive()
  {
    return my_active_token;
  }

  /**
   * Set the alive token in the node.
   * 
   * @param the_alive_token true if the node is connected to the RMI registry,
   *          false if the node is not connected.
   */
  public void setActiveFlag(final boolean the_alive_token)
  {
    my_active_token = the_alive_token;
  }

  /**
   * Find a first key in this node.
   * 
   * @return a first key in this node.
   */
  public String findFirstKey()
  {
    String result = null;
    if (!my_dataset.isEmpty())
    {
      result = my_dataset.first();
    }
    return result;
  }

  /**
   * Find a last key in this node.
   * 
   * @return a last key in the node.
   */
  public String findLastKey()
  {
    String result = null;
    if (!my_dataset.isEmpty())
    {
      result = my_dataset.last();
    }
    return result;
  }

  /**
   * Find a total keys in the node.
   * 
   * @return a total number of keys in this node.
   */
  public int findTotalKeys()
  {
    return my_dataset.size();
  }

}