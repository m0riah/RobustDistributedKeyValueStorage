/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sky Moon
 */

package kvsnode;

import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import util.Log;
import util.LogWriter;
import util.Validator;

/**
 * This class is an actual KVS node, and simulator of the virtual KVS nodes. It
 * has a responsibility to create, maintain, and fix KVS nodes(actual and
 * virtual) in a server. It can be also considered as an actual KVS node in a
 * server by implementing KVSNode.
 * 
 * The node port number for the constructor is used for the port number of an
 * actual node, and it will increment the original port number to use them for
 * the virtual nodes.
 * 
 * @author sky
 * @version 5/3/2013
 */
public class ActualKVSNodeImpl extends AbstractKVSNode
{

  /**
   * To save memory from using "y" more than 2 times.
   */
  public static final String YES = "y";

  /**
   * To save memory from using "n" more than 2 times.
   */
  public static final String NO = "n";

  /**
   * Wait time for the actual node at the initializing phase.
   */
  public static final int WAIT_TIME = 3000;

  /**
   * Wait time to make sure all the nodes in the ring are up. We use 1.5 as a
   * multiplier to make sure all the nodes are up because it should be bigger
   * than the WAIT_TIME, and We are not sure that how much it will be taking to
   * create the virtual nodes for the actual nodes, but we think 1.5 is pretty
   * reasonable number to guarantee that all the nodes are up in this simple
   * local network application.
   */
  public static final int SUCCESSOR_FINDING_WAIT_TIME = (int) (WAIT_TIME * 1.5);

  /**
   * 
   */
  public static final String BACK_UP = "bp";

  /**
   * A token indicating this node fixing the system.
   */
  private boolean my_system_fixing_token;

  /**
   * It has information of the virtual KVS nodes this actual node should deals
   * with (including the virtual nodes). This field is a static to avoid garbage
   * collection. For testing purposes only.
   */
  private final Map<Integer, KVSNode> my_virtual_nodes;

  /**
   * List of all virtual nodes held by this actual node. This ID list is used to
   * reduce a computation to sort the virtual node IDs in clockwise order in the
   * ring system.
   */
  private List<Integer> my_virtual_node_ids;

  /**
   * List of the actual nodes in increasing order in the ring.
   */
  private List<Integer> my_successors;

  /**
   * Backup place for the direct successor actual node.
   */
  private Map<Integer, SortedSet<String>> my_backup_for_successor;

  /**
   * This is a list of IDs of the nodes the successor actual node managing. This
   * extra List data structure is used to easily manage a scale up and down of
   * the backup data process.
   */
  private List<Integer> my_backup_ids_for_successor;

  /**
   * A constructor for actual node.
   * 
   * - related instruction in the instruction document -
   * 
   * The actual nodes in the system are registered with a single RMI registry;
   * this is analogous to hosts on the Internet being registered with the Domain
   * Name System (DNS).Each group will use its own RMI registry. For the sake of
   * non-interference during development and testing, consider ports 80XX (where
   * XX is a team number) reserved for RMI registries; for example, team 03
   * would use port 8003 for their registry on whatever machine they choose to
   * run it on. Despite this reservation, your system should be implemented such
   * that it can use an RMI registry on any host and port. Nodes are named in
   * the RMI registry such that they can be looked up by number (e.g., by
   * appending the number to a fixed string), because nodes in the ring will
   * need to find their successors and neighbors in the registry.
   * 
   * @param the_properties the configuration object for the node.
   */
  public ActualKVSNodeImpl(final Properties the_properties)
  {
    super(the_properties);
    my_virtual_nodes = new HashMap<Integer, KVSNode>();
    my_virtual_node_ids = new LinkedList<Integer>();
    my_successors = new LinkedList<Integer>();
    /*
     * we are using hash map to constant (supposedly, we are not using that many
     * nodes in the ring) access to the ring.
     */
    my_backup_for_successor = new HashMap<Integer, SortedSet<String>>();
    my_backup_ids_for_successor = new LinkedList<Integer>();
    my_system_fixing_token = false;
    locateRMIRegistry();
  }

  /**
   * Locate the RMI registry.
   */
  private void locateRMIRegistry()
  {
    final String hostname = getProperties().getProperty("rmihost");
    final String rmi_port = getProperties().getProperty("rmiport");
    try
    {
      // Check if a valid port number.
      if (Validator.isPortNumber(rmi_port))
      {
        final int rmi_port_int = Integer.valueOf(rmi_port);
        final Registry static_registry = LocateRegistry.getRegistry(hostname, rmi_port_int);
        setRegistry((Registry) static_registry.lookup(REMOTE_REGISTRY));
        Log.out("The KVS node got a remote registry located in host: " + hostname +
                " and port: " + rmi_port + " '" + REMOTE_REGISTRY + "'");
      }
      else
      {
        Log.err("port must be between 1025 and 65535 inclusive");
        System.exit(1);
      }
    }
    catch (final AccessException e)
    {
      Log.err("issue accessing registry: " + e);
      System.exit(1);
    }
    catch (final RemoteException e)
    {
      Log.err("a problem occurred: " + e);
      System.exit(1);
    }
    catch (final ArrayIndexOutOfBoundsException | NumberFormatException e)
    {
      Log.err("Invalid start parameters" + e);
    }
    catch (final NotBoundException e)
    {
      Log.err("Could not find RMI server.");
      System.exit(1);
    }
  }

  /**
   * Run this actual node.
   */
  public void runNode()
  {
    final String build_kvs = getProperties().getProperty("buildKVS");
    final String node_id_str = getProperties().getProperty("nodeid");
    final String m_str = getProperties().getProperty("m");
    if (validateData(node_id_str, m_str))
    {
      final int node_id = Integer.valueOf(node_id_str);
      // Start up new actual node in new ring.
      if (YES.equals(build_kvs))
      {
        locateActualNode(node_id);
        simulateVirtualNodes();
        findSuccessors(node_id, SUCCESSOR_FINDING_WAIT_TIME);
        initiateBackupData();

      }
      // Start up new node in pre-existing ring with existing actual nodes.
      else
      {
        joinRing(node_id);
      }
      Log.out("The actual node is up. You can test within a second.");
    }
    else
    {
      Log.err("Invalid m or node id value in config file. Exiting program.");
      System.exit(1);
    }
  }

  /**
   * Validates the m and node id values in the config file before usage.
   * 
   * @param the_node_data the node id given in the config file
   * @param the_m_data the m value given in the config file
   * @return true if values are valid; false otherwise
   */
  private boolean validateData(final String the_node_data, final String the_m_data)
  {
    boolean valid = false;

    // Check the node number and m value.
    if (Validator.isInteger(the_node_data) && Validator.isInteger(the_m_data))
    {
      final int node_id = Integer.valueOf(the_node_data);
      final int m = Integer.valueOf(the_m_data);
      valid = true;
      if (m >= 0 && node_id >= 0 && node_id < getTotalNodeNumber())
      {
        valid = true;
      }
      else
      {
        valid = false;
      }
    }
    else
    {
      valid = false;
    }

    return valid;
  }

  /**
   * Locate the actual node in the ring system. This method is used when the
   * actual node initiates a ring.
   * 
   * @param the_node_id the id of the actual node
   */
  private void locateActualNode(final int the_node_id)
  {
    try
    {
      getRegistry().bind(String.valueOf(the_node_id),
                         UnicastRemoteObject.exportObject(this, 0));
      // Set to be an actual node id.
      setActualNodeId(the_node_id);
      Log.out("Actual node created at spot " + the_node_id);
      LogWriter.logStartup(the_node_id, Integer.valueOf(getProperties().getProperty("m")));
    }
    catch (final RemoteException e)
    {
      Log.err("a problem occurred: " + e);
      System.exit(1);
    }
    catch (final AlreadyBoundException e)
    {
      Log.out("node " + the_node_id + " is already bound.");
      System.exit(1);
    }
  }

  /**
   * Simulate virtual nodes with no data set when the actual node initiate the
   * KVS ring system.
   * 
   * - related instruction in the instruction document -
   * 
   * The system is started with 2^m nodes (each of which is responsible for
   * storing the values for 1/2^m keys), numbered 0 . . . 2^m - 1. Both m and
   * its own number are known to each node when it starts up. Some of the nodes
   * in the system are virtual and others are actual. For example, a 16 node
   * system might be started with only 4 actual nodes, numbered 2, 5, 9, and 14;
   * each of these actual nodes is responsible for simulating other virtual
   * nodes such that there is a full set of 2^m nodes. With no redundancy, each
   * actual node might be responsible for simulating the nodes numbered higher
   * than it and lower than the next actual node (modulo 2^m ). Thus, actual
   * node 2 would also simulate virtual nodes 3-4, actual node 5 would also
   * simulate virtual nodes 6-8, actual node 9 would also simulate virtual nodes
   * 10- 13, and actual node 14 would also simulate virtual nodes 15, 0, and 1.
   * Later, when you need to implement fault tolerance, you will want each node
   * to be simulated by more than one actual node (so that if one actual node
   * disappears, no data is lost).
   */
  private void simulateVirtualNodes()
  {
    Log.out("Sleeping " + WAIT_TIME + " millisec to let other actual node can come in.");
    try
    {
      /*
       * Wait certain amount of time until it generates virtual nodes because we
       * need to give enough time to let other actual nodes join in to the ring
       * before initiating the KVS ring system.
       */
      Thread.sleep(WAIT_TIME);
    }
    catch (final InterruptedException e1)
    {
      // do nothing
    }
    int i = getRouteTracker().getNextNodeInRing(getNodeId());
    boolean unoccupied_loc = true;
    while (i != getNodeId() && i < getTotalNodeNumber() && unoccupied_loc)
    {
      try
      {
        // Create new virtual node.
        final KVSNode temp_node = new VirtualKVSNodeImpl(getProperties(), i, this);
        // Bind node, add to map.
        getRegistry().bind(String.valueOf(i), UnicastRemoteObject.exportObject(temp_node, 0));
        my_virtual_nodes.put(i, temp_node);
        my_virtual_node_ids.add(i);
        // temp_node.runNode();
        i = getRouteTracker().getNextNodeInRing(i);
      }
      catch (final RemoteException e)
      {
        Log.err(e.toString());
        unoccupied_loc = false;
        System.exit(1);
      }
      catch (final AlreadyBoundException e)
      {
        Log.out("Found bound node, node: " + i);
        unoccupied_loc = false;
      }
    }
    LogWriter.logVirtualNodes(getNodeId(), my_virtual_node_ids);
  }

  /**
   * Find successors for actual nodes.
   * 
   * If this actual node initiate the ring, it waits 1.5 * the time to wait
   * other actual nodes joining to the ring. We assume the 1.5 times are enough
   * to guarantee that there is no more actual node joining after the first
   * actual node joined the ring.
   * 
   * @param the_node_id the actual node id
   * @param the_wait_time wait time before finding the successors.
   */
  public void findSuccessors(final int the_node_id, final int the_wait_time)
  {
    my_successors.clear();
    Log.out("Waiting " + the_wait_time + " millisec to find actual" +
            "node succssors for node " + the_node_id);
    try
    {
      Thread.sleep(the_wait_time);
    }
    catch (final InterruptedException e1)
    {
      // Do nothing.
    }
    final StringBuilder sb = new StringBuilder();
    int i = getRouteTracker().getNextNodeInRing(the_node_id);
    // Search through all the nodes in the ring.
    while (i != the_node_id)
    {
      final KVSNode node = getNode(i);
      try
      {
        if (node.isActualNode())
        {
          my_successors.add(i);
          sb.append(String.valueOf(i) + " ");
        }
      }
      catch (final RemoteException e)
      {
        Log.err(e.toString());
      }
      i = getRouteTracker().getNextNodeInRing(i);
    }
    // log successors
    LogWriter.logSuccessor(getNodeId(), my_successors);
  }

  /**
   * This is to initiate backup data Map and List using successor list. This
   * method should be only invoked when a new actual node is set up (Initiating
   * a ring).
   */
  private void initiateBackupData()
  {
    if (my_successors.size() == 0)
    {
      // Do not need backup.
      Log.out("None successor actual node case");
    }
    else if (my_successors.size() == 1)
    {
      /*
       * If it has only one successor, then store the backup data up to before
       * the node id itself.
       */
      final int first_successor = my_successors.get(0);
      Log.out("One successor actual node case: " + first_successor);
      my_backup_ids_for_successor.add(first_successor);
      my_backup_for_successor.put(first_successor, new TreeSet<String>());
      int i = getRouteTracker().getNextNodeInRing(first_successor);
      while (i != getNodeId())
      {
        my_backup_ids_for_successor.add(i);
        my_backup_for_successor.put(i, new TreeSet<String>());
        i = getRouteTracker().getNextNodeInRing(i);
      }
    }
    else
    {
      /*
       * If there are more than two successors, store backup data between its
       * first successor and the second successor (first successor included).
       */
      final int first_successor = my_successors.get(0);
      final int second_successor = my_successors.get(1);
      Log.out("Multi successor actual node case: " + first_successor + " and " +
              second_successor);
      my_backup_ids_for_successor.add(first_successor);
      my_backup_for_successor.put(first_successor, new TreeSet<String>());
      int i = getRouteTracker().getNextNodeInRing(first_successor);
      // second successor excluded.
      while (i != second_successor)
      {
        my_backup_ids_for_successor.add(i);
        my_backup_for_successor.put(i, new TreeSet<String>());
        i = getRouteTracker().getNextNodeInRing(i);
      }
    }
    LogWriter.logBackupData(getNodeId(), my_backup_ids_for_successor);
  }

  /**
   * Join the KVS ring system with this actual node.
   * 
   * @param the_node_id a node id to take the position over.
   * @return true if the process was successful, false if not.
   */
  private boolean joinRing(final int the_node_id)
  {
    final KVSNode target_node = getNode(the_node_id);
    try
    {
      final Map<String, Collection<String>> data = target_node.joinToRing();
      // code key is the location having the process code.
      if (data.get("code").contains("declined"))
      {
        Log.err("The claimed node id is occupied by an actual node. "
                + "You cannot take over an actual node. try other nodes.");
      }
      else
      {
        data.remove("code");
        setActualNodeId(the_node_id);
        /*
         * set data set in this actual node. the if there is no key indicating
         * the id of this actual node, then we assume that the data is
         * corrupted.
         */
        if (data.containsKey(String.valueOf(getNodeId())))
        {
          LogWriter.logStartup(getNodeId(), Integer.valueOf(getProperties().getProperty("m")));
          final List<String> passed_vnode_list = (ArrayList<String>) data.get("vnodelist");
          final SortedSet<String> dataset =
              (TreeSet<String>) data.get(String.valueOf(getNodeId()));
          this.setCoreDataSet(dataset);
          data.remove(String.valueOf(getNodeId()));
          passed_vnode_list.remove(String.valueOf(getNodeId()));
          final Iterator<String> vnode_itr = passed_vnode_list.iterator();
          final List<Integer> vnode_ids = new LinkedList<Integer>();
          while (vnode_itr.hasNext())
          {
            final String vnode_id = vnode_itr.next();
            vnode_ids.add(Integer.valueOf(vnode_id));
          }
          my_virtual_node_ids = vnode_ids;
          LogWriter.logVirtualNodes(getNodeId(), my_virtual_node_ids);
          data.remove("vnodelist");
          // set successor actual nodes
          final Iterator<String> successor_itr = data.get("successors").iterator();
          final List<Integer> successors = new LinkedList<Integer>();
          while (successor_itr.hasNext())
          {
            successors.add(Integer.valueOf(successor_itr.next()));
          }
          my_successors = successors;
          data.remove("successors");
          LogWriter.logSuccessor(getNodeId(), my_successors);
          // bind this node to RMI registry
          getRegistry().rebind(String.valueOf(getNodeId()),
                               UnicastRemoteObject.exportObject(this, 0));
          Log.out("Actual node joined at spot " + getNodeId());
          // set backup data.
          updateWholeBackup(data);
          // simulate virtual nodes with given data array.
          simulateVirtualNodesWithData(data);
        }
        else
        {
          Log.err("Got currupted data from its predecessor actual node, join failed");
          System.exit(1);
        }
      }
    }
    catch (final RemoteException e)
    {
      Log.err(e.toString());
    }
    return true;
  }

  /**
   * Update the backup data with the data stored in its successor actual node.
   * Can be used in join, and accident leave situation to grab data in its
   * successor actual node.
   * 
   * @param the_data a map having the backup data.
   * @return true to make a caller of this method to wait until the computation
   *         of this method is over.
   */
  public boolean updateWholeBackup(final Map<String, Collection<String>> the_data)
  {
    my_backup_for_successor.clear();
    my_backup_ids_for_successor.clear();
    final Iterator<String> backup_id_itr = the_data.get(BACKUP_LIST).iterator();
    while (backup_id_itr.hasNext())
    {
      final String key = backup_id_itr.next();
      my_backup_ids_for_successor.add(Integer.valueOf(key));
      final SortedSet<String> backup = (TreeSet<String>) the_data.get(BACK_UP + key);
      my_backup_for_successor.put(Integer.valueOf(key), backup);
      the_data.remove(BACK_UP + key);
    }
    the_data.remove(BACKUP_LIST);
    LogWriter.logBackupData(getNodeId(), my_backup_ids_for_successor);
    return true;
  }

  /**
   * Update the backup data with the data stored in its successor actual node.
   * Can be used in join, and accident leave situation to grab data in its
   * successor actual node.
   * 
   * @param the_data a map having the backup data.
   * @return true to make a caller of this method to wait until the computation
   *         of this method is over.
   */
  public boolean updateBackupDataFromSucToPre(final Map<String, Collection<String>> the_data)
  {
    my_backup_for_successor.clear();
    my_backup_ids_for_successor.clear();
    final Iterator<String> backup_id_itr = the_data.get(BACKUP_LIST).iterator();
    while (backup_id_itr.hasNext())
    {
      final String key = backup_id_itr.next();
      my_backup_ids_for_successor.add(Integer.valueOf(key));
      final SortedSet<String> backup = (TreeSet<String>) the_data.get(BACK_UP + key);
      my_backup_for_successor.put(Integer.valueOf(key), backup);
      the_data.remove(BACK_UP + key);
    }
    the_data.remove(BACKUP_LIST);
    LogWriter.logBackupData(getNodeId(), my_backup_ids_for_successor);
    return true;
  }

  /**
   * Simulate virtual nodes with data set.
   * 
   * @param the_data a data set having all data to set up virtual nodes.
   * @return true if the process is successful, false if not.
   */
  private boolean simulateVirtualNodesWithData(final Map<String, Collection<String>> the_data)
  {
    final Iterator<String> vnode_id_itr = the_data.keySet().iterator();
    while (vnode_id_itr.hasNext())
    {
      final String stringid = vnode_id_itr.next();
      final int vnode_id = Integer.valueOf(stringid);
      final KVSNode temp_node = new VirtualKVSNodeImpl(getProperties(), vnode_id, this);
      final SortedSet<String> dataset = (TreeSet<String>) the_data.get(stringid);
      ((VirtualKVSNodeImpl) temp_node).setCoreDataSet(dataset);
      try
      {
        getRegistry().rebind(stringid, UnicastRemoteObject.exportObject(temp_node, 0));
      }
      catch (final RemoteException e)
      {
        Log.err(e.toString());
      }
      my_virtual_nodes.put(vnode_id, temp_node);
    }
    return true;
  }

  /**
   * Find KVSNode based on node id in the registry.
   * 
   * @param the_node_id the node id to use
   * @return KVSnode
   */
  private KVSNode getNode(final int the_node_id)
  {
    KVSNode node = null;
    try
    {
      node = (KVSNode) getRegistry().lookup(Integer.toString(the_node_id));
    }
    catch (final AccessException e)
    {
      Log.err("Actual node finding: issue accessing registry: " + e);
      System.exit(1);
    }
    catch (final RemoteException e)
    {
      Log.err("Actual node finding: a problem occurred: " + e);
      System.exit(1);
    }
    catch (final NotBoundException e)
    {
      Log.err("Actual node finding: Could not find RMI server.");
      System.exit(1);
    }
    return node;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Collection<String>> joinToRing() throws RemoteException
  {
    final Map<String, Collection<String>> result = new HashMap<String, Collection<String>>();
    final List<String> list = new ArrayList<String>();
    list.add("declined");
    result.put("code", list);
    return result;
  }

  /**
   * Extract data from the removing virtual node to pass it to the new actual
   * node.
   * 
   * @param the_virtual_node an ID of virtual node the join() method was
   *          invocated on.
   * @return a data 2D array to let the actual node initialize the join process
   *         with.
   */
  public Map<String, Collection<String>> getDataForJoin(final int the_virtual_node)
  {
    final int first_vnode_index = my_virtual_node_ids.indexOf(the_virtual_node);
    // make the message processing flag and alive tokens false in the virtual
    // nodes.
    for (int i = first_vnode_index; i < my_virtual_node_ids.size(); i++)
    {
      final KVSNode temp_node = my_virtual_nodes.get(my_virtual_node_ids.get(i));
      // ((AbstractKVSNode) temp_node).setMessageProcessFlag(false);
      ((AbstractKVSNode) temp_node).setActiveFlag(false);
    }
    final Map<String, Collection<String>> data = new HashMap<String, Collection<String>>();
    // put backup data and list of backup data IDs.
    data.putAll(getBackUpDataForJoin(the_virtual_node));
    // add a list of successor actual nodes for the new actual node.
    final List<String> successors = new ArrayList<String>();
    final Iterator<Integer> successor_itr = my_successors.iterator();
    while (successor_itr.hasNext())
    {
      final int suc = successor_itr.next();
      successors.add(String.valueOf(suc));
    }
    // put this actual node number at the last of the successor list, which
    // means that this node is a direct predecessor.
    successors.add(String.valueOf(getNodeId()));
    data.put("successors", successors);
    // generate a process code: simply the index of the_virtual_node
    final List<String> code = new ArrayList<String>();
    code.add("OK");
    data.put("code", code);
    // extract all the data from the virtual nodes which are going to be taken
    // over.
    final List<String> new_vnode_id_list = new ArrayList<String>();
    for (int i = first_vnode_index; i < my_virtual_node_ids.size(); i++)
    {
      final int vnode_id = my_virtual_node_ids.get(i);
      // this line is very important to maintain the order of the new virtual
      // node list.
      new_vnode_id_list.add(String.valueOf(vnode_id));
      // grab a data set from a virtual nodes then put it into the map
      final KVSNode temp_node = my_virtual_nodes.get(vnode_id);
      if (vnode_id == ((AbstractKVSNode) temp_node).getNodeId())
      {
        data.put(String.valueOf(vnode_id), ((AbstractKVSNode) temp_node).getCoreDataSet());
      }
    }
    data.put("vnodelist", new_vnode_id_list);
    final int checking_node = my_virtual_node_ids.get(my_virtual_node_ids.size() - 1);
    // check the rebounds by new virtual nodes simulated by predecessor.
    final Thread cfnr =
        new Thread(
                   new CheckerForNodeRebound(this, ReboundCheckingCase.JOIN, checking_node,
                                             -1, my_virtual_node_ids.indexOf(the_virtual_node)));
    cfnr.start();
    return data;
  }

  /**
   * Get backup data for the new joining node.
   * 
   * @param the_virtual_node an ID of virtual node the join() method was
   *          invocated on.
   * @return a map having list of the backup data id and id/backup data as
   *         key/value pair.
   */
  private Map<String, Collection<String>> getBackUpDataForJoin(final int the_virtual_node)
  {
    final int first_vnode_index = my_virtual_node_ids.indexOf(the_virtual_node);
    final Map<String, Collection<String>> backup = new HashMap<String, Collection<String>>();
    // this is to put my_backup_ids_for_successor.
    final List<String> backup_list = new LinkedList<String>();
    if (my_successors.isEmpty())
    {
      // Put all data in itself and virtual nodes up to the value of
      // the_virtual_node using BACK_UP prefix.
      backup_list.add(String.valueOf(getNodeId()));
      backup.put(BACK_UP + getNodeId(), getCoreDataSet());
      for (int i = 0; i < first_vnode_index; i++)
      {
        final int vnode_id = my_virtual_node_ids.get(i);
        // this line is very important to maintain the order of the new virtual
        // node list.
        backup_list.add(String.valueOf(vnode_id));
        // grab a data set from a virtual nodes then put it into the map
        final KVSNode temp_node = my_virtual_nodes.get(vnode_id);
        if (vnode_id == ((AbstractKVSNode) temp_node).getNodeId())
        {
          backup.put(BACK_UP + vnode_id, ((AbstractKVSNode) temp_node).getCoreDataSet());
        }
      }
    }
    else
    {
      final Iterator<Integer> backup_id_itr = my_backup_ids_for_successor.iterator();
      while (backup_id_itr.hasNext())
      {
        final int node_id = backup_id_itr.next();
        backup_list.add(String.valueOf(node_id));
        backup.put(BACK_UP + node_id, my_backup_for_successor.get(node_id));
      }
    }
    backup.put(BACKUP_LIST, backup_list);
    return backup;
  }

  /**
   * It simply removes all the nodes after the_deleting_node_index.
   * 
   * @param the_deleting_node_index a message processing code.
   */
  public void removeVirtualNodesForJoin(final int the_deleting_node_index)
  {
    Log.out("Removing unnessary virtual nodes.");
    // update backup data for this node.
    updateBackUpDataAfterJoiningNodeUp(the_deleting_node_index);
    // resume the message processes in the virtual nodes.
    for (int i = the_deleting_node_index; i < my_virtual_node_ids.size(); i++)
    {
      final KVSNode node = my_virtual_nodes.get(my_virtual_node_ids.get(i));
      ((AbstractKVSNode) node).setMessageProcessFlag(true);
    }
    // Wait until there is no message to process in the virtual nodes, then
    // remove the nodes.
    final int start = my_virtual_node_ids.size() - 1;
    for (int j = start; j >= the_deleting_node_index; j--)
    {
      final KVSNode node = my_virtual_nodes.get(my_virtual_node_ids.get(j));
      boolean busy = !((AbstractKVSNode) node).isTransferQueueEmpty();
      while (busy)
      {
        busy = !((AbstractKVSNode) node).isTransferQueueEmpty();
      }
      my_virtual_nodes.remove(my_virtual_node_ids.get(j));
      my_virtual_node_ids.remove(j);
    }
    LogWriter.logVirtualNodes(getNodeId(), my_virtual_node_ids);
    findSuccessors(getNodeId(), 0);
    LogWriter.logSuccessor(getNodeId(), my_successors);
    sendOutToInformSuccessorUpdate();
    // If the current number of successors is bigger than 1, then we need to
    // scale down the predecessor of this actual node.
    if (my_successors.size() > 1)
    {
      invokeScaleDownBackupDataToPredecessor();
    }
  }

  /**
   * Update its backup data after the join by a new actual node got successful.
   * 
   * @param the_deleting_node_index a index of the virtual node the new actual
   *          node took.
   */
  public void updateBackUpDataAfterJoiningNodeUp(final int the_deleting_node_index)
  {
    // update the backup data in this node.
    my_backup_for_successor.clear();
    my_backup_ids_for_successor.clear();
    final int last = my_virtual_node_ids.size() - 1;
    for (int i = last; i >= the_deleting_node_index; i--)
    {
      final KVSNode node = my_virtual_nodes.get(my_virtual_node_ids.get(i));
      ((LinkedList<Integer>) my_backup_ids_for_successor).addFirst(my_virtual_node_ids.get(i));
      my_backup_for_successor.put(my_virtual_node_ids.get(i),
                                  ((AbstractKVSNode) node).getCoreDataSet());
    }
    LogWriter.logBackupData(getNodeId(), my_backup_ids_for_successor);
  }

  /**
   * Send out messages to let other node update their successors.
   * 
   * @return true to let the caller of this method wait until the computation in
   *         the method is done.
   */
  public boolean sendOutToInformSuccessorUpdate()
  {
    // Let other actual nodes update their successor again.
    final Iterator<Integer> successor_itr = my_successors.iterator();
    while (successor_itr.hasNext())
    {
      final int successor = successor_itr.next();
      final RemoteMessage informing_update_messages =
          new RemoteMessage(getNodeId(), -1, successor, false, QueryType.UPDATE_SUCCESSORS,
                            -1, null, null);
      final int next_hop = getRouteTracker().getNextHop(successor, getNodeId());
      try
      {
        final KVSNode next_node = (KVSNode) getRegistry().lookup(String.valueOf(next_hop));
        next_node.sendMessage(getNodeId(), getNodeId(), informing_update_messages);
      }
      catch (final RemoteException | NotBoundException e)
      {
        Log.err("Cannot get KVSNode " + next_hop + " from the RMI registry.");
      }
    }
    return true;
  }

  /**
   * In case that the actual node reduces its number of nodes, the predecessor
   * also needs to scale down its backup storage.
   * 
   * @return true if the process is successful, false else.
   */
  private boolean invokeScaleDownBackupDataToPredecessor()
  {
    final int pred_id = ((LinkedList<Integer>) my_successors).getLast();
    final Map<String, Collection<String>> backup_data =
        new HashMap<String, Collection<String>>();
    final List<String> backup_list = new LinkedList<String>();
    backup_list.add(String.valueOf(getNodeId()));
    final Iterator<Integer> itr = my_virtual_node_ids.iterator();
    while (itr.hasNext())
    {
      backup_list.add(String.valueOf(itr.next()));
    }
    backup_data.put(BACKUP_LIST, backup_list);
    final RemoteMessage scaleup_message =
        new RemoteMessage(getNodeId(), -1, pred_id, false, QueryType.BACKUP_ALL_IN_PRED, -1,
                          null, backup_data);
    final int nex_hop = getRouteTracker().getNextHop(pred_id, getNodeId());
    final KVSNode predecessor = getNode(nex_hop);
    try
    {
      predecessor.sendMessage(getNodeId(), getNodeId(), scaleup_message);
    }
    catch (final RemoteException e)
    {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String invokeNodeToLeave() throws RemoteException
  {
    String result = "";
    if (my_successors.isEmpty())
    {
      result = "This node is only actual node, cannot leave.";
      LogWriter.logAttemptedLeaving(getNodeId());
    }
    else
    {
      // stop the message process in this node, and make the alive token false.
      setActiveFlag(false);
      final Iterator<Integer> itr = my_virtual_node_ids.iterator();
      // stop the message process in its virtual nodes, and make the alive token
      // false.
      while (itr.hasNext())
      {
        final int vnode_id = itr.next();
        final KVSNode vnode = my_virtual_nodes.get(vnode_id);
        ((AbstractKVSNode) vnode).setActiveFlag(false);
      }
      // get predecessor node.
      final int checking_node = my_virtual_node_ids.get(my_virtual_node_ids.size() - 1);
      final int pre_node_id = ((LinkedList<Integer>) my_successors).getLast();
      final int next_hop = getRouteTracker().getNextHop(pre_node_id, getNodeId());
      final KVSNode next_node = getNode(next_hop);
      final RemoteMessage nodes_data =
          new RemoteMessage(getNodeId(), -1, pre_node_id, false, QueryType.VNODES_DATA, -1,
                            null, getAllDataForLeaving());
      next_node.sendMessage(getNodeId(), getNodeId(), nodes_data);
      // check the rebounds by new virtual nodes simulated by predecessor.
      final Thread cfnr =
          new Thread(new CheckerForNodeRebound(this, ReboundCheckingCase.LEAVING,
                                               checking_node, pre_node_id, -1));
      cfnr.start();
      result = "The actual node " + getNodeId() + " will be leaving soon.";
    }
    return result;
  }

  /**
   * Grab all the data this actual node had from itself and its virtual nodes,
   * then put them into a hash map.
   * 
   * @return data map.
   */
  private Map<String, Collection<String>> getAllDataForLeaving()
  {
    final Map<String, Collection<String>> data = new HashMap<String, Collection<String>>();
    final List<String> passing_node_id_list = new ArrayList<String>();
    // put data set in this actual node into the map
    data.put(String.valueOf(getNodeId()), getCoreDataSet());
    passing_node_id_list.add(String.valueOf(getNodeId()));
    // extract all the data from the virtual nodes which are going to be taken
    // over.
    final Iterator<Integer> vnode_id_itr = my_virtual_node_ids.iterator();
    while (vnode_id_itr.hasNext())
    {
      final int key = vnode_id_itr.next();
      final KVSNode vnode = my_virtual_nodes.get(key);
      data.put(String.valueOf(key), ((AbstractKVSNode) vnode).getCoreDataSet());
      passing_node_id_list.add(String.valueOf(key));
    }
    data.put("vnodelist", passing_node_id_list);
    data.putAll(getBackupDataForLeaving());
    return data;
  }

  /**
   * Get backup data in a map. If there are at least three actual nodes in the
   * ring, then pass the whole backup data in the leaving node to its
   * predecessor. If there is only two nodes (only one node is out of
   * consideration), then pass an empty data map because after the node leave
   * there is only one actual node, and one actual node does not need to
   * maintain backup data.
   * 
   * @return a backup data map.
   */
  private Map<String, Collection<String>> getBackupDataForLeaving()
  {
    final Map<String, Collection<String>> backup = new HashMap<String, Collection<String>>();
    // this is to put my_backup_ids_for_successor.
    final List<String> backup_list = new LinkedList<String>();
    // happens when there are at least three actual nodes in the ring.
    if (my_successors.size() > 0)
    {
      final Iterator<Integer> backup_id_itr = my_backup_ids_for_successor.iterator();
      while (backup_id_itr.hasNext())
      {
        final int node_id = backup_id_itr.next();
        backup_list.add(String.valueOf(node_id));
        backup.put(BACK_UP + node_id, my_backup_for_successor.get(node_id));
      }
      backup.put(BACKUP_LIST, backup_list);
    }
    /*
     * else just pass empty one. 2 - 1 = 1 actual node in the ring. So no need
     * to maintain backup data.
     */
    return backup;
  }

  /**
   * Resume all the message process to empty all blocking queues in this node
   * and its virtual nodes, then send update successor message to other
   * successors, then leaven the ring.
   * 
   * @return true. it is mainly to a caller of this method wait until the
   *         computation finishes.
   */
  public boolean removeAllNode()
  {
    // resume the message processes in the virtual nodes.
    setMessageProcessFlag(true);
    Iterator<Integer> vnode_id_itr = my_virtual_node_ids.iterator();
    while (vnode_id_itr.hasNext())
    {
      final int key = vnode_id_itr.next();
      final KVSNode vnode = my_virtual_nodes.get(key);
      ((AbstractKVSNode) vnode).setMessageProcessFlag(true);
    }
    // confirm there is no message in the blocking queues.
    boolean busy = !isTransferQueueEmpty();
    while (busy)
    {
      busy = !isTransferQueueEmpty();
    }
    vnode_id_itr = my_virtual_node_ids.iterator();
    while (vnode_id_itr.hasNext())
    {
      final int key = vnode_id_itr.next();
      final KVSNode vnode = my_virtual_nodes.get(key);
      busy = !((AbstractKVSNode) vnode).isTransferQueueEmpty();
      while (busy)
      {
        busy = !((AbstractKVSNode) vnode).isTransferQueueEmpty();
      }
    }
    // Let other actual nodes update their successor again.
    final Iterator<Integer> successor_itr = my_successors.iterator();
    while (successor_itr.hasNext())
    {
      final int successor = successor_itr.next();
      final RemoteMessage informing_update_messages =
          new RemoteMessage(getNodeId(), -1, successor, false, QueryType.UPDATE_SUCCESSORS,
                            -1, null, null);
      final int next_hop = getRouteTracker().getNextHop(successor, getNodeId());
      try
      {
        final KVSNode next_node = (KVSNode) getRegistry().lookup(String.valueOf(next_hop));
        next_node.sendMessage(getNodeId(), getNodeId(), informing_update_messages);
      }
      catch (final RemoteException | NotBoundException e)
      {
        Log.err("Cannot get KVSNode " + next_hop + " from the RMI registry.");
      }
    }
    return true;
  }

  /**
   * To simulate virtual nodes using data passed from leaving node.
   * 
   * @param the_datasets a data set to simulate virtual nodes.
   * @return true if the process is successful, false if not.
   */
  public boolean simulateVNodeWithData(final Map<String, Collection<String>> the_datasets)
  {
    final long time = System.currentTimeMillis();
    while (System.currentTimeMillis() - time < CLIENT_QUERY_WAIT_TIME)
    {
      try
      {
        Thread.sleep(CLIENT_QUERY_WAIT_TIME);
      }
      catch (final InterruptedException e)
      {
        // Do nothing.
      }
    }
    final Iterator<String> vnode_itr = the_datasets.get("vnodelist").iterator();
    final List<Integer> vnode_ids = new LinkedList<Integer>();
    while (vnode_itr.hasNext())
    {
      final String vnode_id = vnode_itr.next();
      vnode_ids.add(Integer.valueOf(vnode_id));
    }
    my_virtual_node_ids.addAll(vnode_ids);
    LogWriter.logVirtualNodes(getNodeId(), my_virtual_node_ids);
    the_datasets.remove("vnodelist");
    // update backup data in the predecessor (receiver).
    updateWholeBackup(the_datasets);
    // simulate virtual nodes in the predecessor (receiver).
    simulateVirtualNodesWithData(the_datasets);
    // Then pass the expended data to its predecessor.
    invokeScaleUpBackupDataInPredecessor(vnode_ids);
    findSuccessors(getNodeId(), 0);
    sendOutToInformSuccessorUpdate();
    Log.out("Okay, The system is now stable.");
    return true;
  }

  /**
   * In case that the actual node expands its number of nodes, the predecessor
   * also needs to scale up its backup storage.
   * 
   * @param the_backup_ids a list backup IDs which will be added to the backup
   *          set in the predecessor.
   * @return true if the process is successful, false else.
   */
  private boolean invokeScaleUpBackupDataInPredecessor(final List<Integer> the_backup_ids)
  {
    final int pred_id = ((LinkedList<Integer>) my_successors).getLast();
    final Map<String, Collection<String>> backup_data =
        new HashMap<String, Collection<String>>();
    final List<String> backup_list = new LinkedList<String>();
    final Iterator<Integer> itr = the_backup_ids.iterator();
    while (itr.hasNext())
    {
      final int node_id = itr.next();
      backup_list.add(String.valueOf(node_id));
      final SortedSet<String> dataset =
          ((AbstractKVSNode) my_virtual_nodes.get(node_id)).getCoreDataSet();
      backup_data.put(BACK_UP + node_id, dataset);
    }
    backup_data.put(BACKUP_LIST, backup_list);
    final RemoteMessage scaleup_message =
        new RemoteMessage(getNodeId(), -1, pred_id, false, QueryType.BACKUP_ALL_IN_PRED, 10,
                          null, backup_data);
    final int nex_hop = getRouteTracker().getNextHop(pred_id, getNodeId());
    final KVSNode predecessor = getNode(nex_hop);
    try
    {
      predecessor.sendMessage(getNodeId(), getNodeId(), scaleup_message);
    }
    catch (final RemoteException e)
    {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * This is to update backup data in an actual node. We use extra List
   * parameter to make the keys(node IDs) in order in the ring system to easily
   * accomplish both scale down and up of the backup data without complicated
   * computation. To scale down the backup data in the node, a passing Map
   * parameter has nothing in it, but the List parameter has one node ID which
   * is the last node ID the invoked node should maintain. After the ID in the
   * maintaining my_backup_ids_for_successor, remove all backup data. To scale
   * up, passing List parameter has ordered node IDs in the ring system. It can
   * simply be added to the my_backup_ids_for_successor to the end of the list
   * in the invoked node. Then add all the backup data in the Map parameter.
   * 
   * @param the_backup a data set to update backup storage in this node.
   * @return true if all the process ends successfully, false if not.
   */
  public boolean updateBackupData(final Map<String, Collection<String>> the_backup)
  {
    boolean result;
    final List<String> backup_list_str = (LinkedList<String>) the_backup.get(BACKUP_LIST);
    final int comming_first =
        Integer.valueOf(((LinkedList<String>) backup_list_str).getFirst());
    final int backup_first = ((LinkedList<Integer>) my_backup_ids_for_successor).getFirst();
    if (comming_first == backup_first)
    {
      final String last_id = ((LinkedList<String>) backup_list_str).getLast();
      // If the first node id in passing list is equal to the first node ID in
      // the backup list, then Scale down.
      result = scaleDownBackUpData(Integer.valueOf(last_id));
    }
    else
    {
      // If the first node id in passing list is equal to the last node ID in
      // the backup list + 1, then Scale up.
      final Iterator<String> itr = backup_list_str.iterator();
      while (itr.hasNext())
      {
        final String key = itr.next();
        my_backup_ids_for_successor.add(Integer.valueOf(key));
        final SortedSet<String> backup = (TreeSet<String>) the_backup.get(BACK_UP + key);
        my_backup_for_successor.put(Integer.valueOf(key), backup);
      }
      result = true;
    }
    LogWriter.logBackupData(getNodeId(), my_backup_ids_for_successor);
    return result;
  }

  /**
   * Scale down the back up data.
   * 
   * @param the_last_id the last id of a node which is should be maintained.
   * @return true if the process ends successfully, false else.
   */
  private boolean scaleDownBackUpData(final int the_last_id)
  {
    final int start = my_backup_ids_for_successor.size() - 1;
    final int deleting_index = my_backup_ids_for_successor.indexOf(the_last_id);
    for (int i = start; i > deleting_index; i--)
    {
      my_backup_for_successor.remove(my_backup_ids_for_successor.get(i));
      my_backup_ids_for_successor.remove(i);
    }
    return true;
  }

  /**
   * Get backup data from its successor. This method is only invoked to restore
   * ring system in case that an actual node accidently left. In normal leaving
   * case, the backup data stored in the leaving node is passed when the leaving
   * node passes all its data. However, the accident leaving case, we cannot get
   * the backup data from the left node (while except the backup data, all data
   * stored in the left node have been stored in its predecessor). For that
   * reason, we added this method to get the backup data from its successor
   * quickly to restore the whole broken system fast.
   * 
   * @param the_receiver a receiver node.
   * @return backup data stored in the successor.
   */
  public boolean sendBackupData(final int the_receiver)
  {
    final List<String> backup_id_list = new LinkedList<String>();
    final Map<String, Collection<String>> backup = new HashMap<String, Collection<String>>();
    // halt the message process.
    // setMessageProcessFlag(false);
    // add data in this node.
    backup_id_list.add(String.valueOf(getNodeId()));
    backup.put(BACK_UP + getNodeId(), getCoreDataSet());
    final Iterator<Integer> itr = my_virtual_node_ids.iterator();
    // setMessageProcessFlag(true);
    while (itr.hasNext())
    {
      final int node_id = itr.next();
      backup_id_list.add(String.valueOf(node_id));
      // ((AbstractKVSNode)
      // my_virtual_nodes.get(node_id)).setMessageProcessFlag(false);
      backup.put(BACK_UP + node_id,
                 ((AbstractKVSNode) my_virtual_nodes.get(node_id)).getCoreDataSet());
      // ((AbstractKVSNode)
      // my_virtual_nodes.get(node_id)).setMessageProcessFlag(true);
    }
    backup.put("backuplist", backup_id_list);
    final RemoteMessage backup_msg =
        new RemoteMessage(getNodeId(), -1, the_receiver, false, QueryType.BACKUP_ALL_IN_SUCC,
                          -1, null, backup);
    final int next_hop = getRouteTracker().getNextHop(the_receiver, getNodeId());
    final KVSNode next_node = getNode(next_hop);
    try
    {
      next_node.sendMessage(getNodeId(), getNodeId(), backup_msg);
    }
    catch (final RemoteException e)
    {
      e.printStackTrace();
    }
    final List<Integer> for_log = new LinkedList<Integer>();
    for_log.add(getNodeId());
    for_log.addAll(my_virtual_node_ids);
    LogWriter.logBackedUpValueSent(getNodeId(), the_receiver, for_log);
    return true;
  }

  /**
   * The node invoked this method immediately restore the failure nodes in its
   * direct successor. Predecessor actual nodes always maintain (except there is
   * one actual node in the system) backup data for their direct successors.
   * 
   * @return true if the process is successful, false if not.
   */
  public boolean restoreFailureNodes()
  {
    if (!my_system_fixing_token)
    {
      // test the successor is really broken.
      final KVSNode successor = getNode(my_successors.get(0));
      boolean test = false;
      try
      {
        successor.isActive();
      }
      catch (final RemoteException e)
      {
        test = true;
      }
      if (test)
      {
        Log.out("Fixing has been started in node " + getNodeId());
        my_system_fixing_token = true;
        // remove the dead successor from the successor list
        final List<Integer> scale_up_list = new LinkedList<Integer>();
        final Iterator<Integer> vnode_itr = my_backup_ids_for_successor.iterator();
        // simulate and bind the failure nodes.
        while (vnode_itr.hasNext())
        {
          final int vnode_id = vnode_itr.next();
          scale_up_list.add(vnode_id);
          final KVSNode temp_node = new VirtualKVSNodeImpl(getProperties(), vnode_id, this);
          ((AbstractKVSNode) temp_node).setCoreDataSet(my_backup_for_successor.get(vnode_id));
          // halt message processing in the creating nodes.
          try
          {
            getRegistry().rebind(String.valueOf(vnode_id),
                                 UnicastRemoteObject.exportObject(temp_node, 0));
          }
          catch (final RemoteException e)
          {
            Log.err(e.toString());
          }
          my_virtual_nodes.put(vnode_id, temp_node);
        }
        findSuccessors(getNodeId(), 0);
        sendOutToInformSuccessorUpdate();
        LogWriter.logSystemRestored(getNodeId(), my_backup_ids_for_successor);
        my_virtual_node_ids.addAll(my_backup_ids_for_successor);
        LogWriter.logVirtualNodes(getNodeId(), my_virtual_node_ids);
        updateBackUpInBothPreAndSuc(scale_up_list);
        // update the backup data in its predecessor and successor.
        // Finished, change the token "not fixing".
        my_system_fixing_token = false;
      }
    }
    return true;
  }

  /**
   * Update backup data in its predecessor, then its successor actual nodes.
   * 
   * @param the_backup_list a list of node IDs to invoke backup updates to its
   *          successor and predecessor.
   * @return true if the process has been successful, false if not.
   */
  public boolean updateBackUpInBothPreAndSuc(final List<Integer> the_backup_list)
  {
    // if there is no successor in the ring, empty the backup storage.
    if (my_successors.size() == 0)
    {
      my_backup_for_successor.clear();
      my_backup_ids_for_successor.clear();
    }
    else
    {
      invokeScaleUpBackupDataInPredecessor(the_backup_list);
      final int suc_id = ((LinkedList<Integer>) my_successors).getFirst();
      final RemoteMessage backup_msg =
          new RemoteMessage(getNodeId(), -1, suc_id, false, QueryType.BACKUP_REQUEST, -1,
                            null, null);
      final int next_hop = getRouteTracker().getNextHop(suc_id, getNodeId());
      final KVSNode next_node = getNode(next_hop);
      try
      {
        next_node.sendMessage(getNodeId(), getNodeId(), backup_msg);
      }
      catch (final RemoteException e)
      {
        e.printStackTrace();
      }
    }
    return true;
  }

  /**
   * Get a backup data IDs.
   * 
   * @return a list having node IDs stored in the Map in order.
   */
  public List<Integer> getBackupIds()
  {
    return my_backup_ids_for_successor;
  }

  /**
   * Get a backup data map.
   * 
   * @return a map having node id and data set as a key/value pair.
   */
  public Map<Integer, SortedSet<String>> getBackupData()
  {
    return my_backup_for_successor;
  }

  /**
   * Get a successor list.
   * 
   * @return a successor list.
   */
  public List<Integer> getSuccessors()
  {
    return my_successors;
  }

  /**
   * Get a map of virtual nodes.
   * 
   * @return a map of virtual nodes.
   */
  public Map<Integer, KVSNode> getVirtualNodes()
  {
    return my_virtual_nodes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getFirstKey() throws RemoteException
  {
    final int msg_id = getMessageId();
    getSentMessageTable().put(msg_id, new Result(Thread.currentThread()));
    calculateFirstKeyThenSendOut(null, msg_id, getNodeId());
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
    String result = null;
    if (getSentMessageTable().get(msg_id).isResultArrived())
    {
      // check sent message table to get the result.
      final String ring_first_key = (String) getSentMessageTable().get(msg_id).getResult();
      result = ring_first_key;
      LogWriter.logFirstKey(getNodeId(), result);
      getSentMessageTable().get(msg_id).setResultPassed(true);
    }
    return result;
  }

  /**
   * This method calculates a local first key, then compare with the first key
   * in the ring which has been calculated, then pass to next successor.
   * 
   * @param the_ring_first_key a calculated values in the predecessor actual
   *          nodes.
   * @param the_msg_id a message id of this process.
   * @param the_final_dest a final destination node of this calculation.
   */
  public void calculateFirstKeyThenSendOut(final String the_ring_first_key,
                                           final int the_msg_id, final int the_final_dest)
  {
    final Iterator<Integer> itr = my_virtual_nodes.keySet().iterator();
    String local_first_key = findFirstKey();
    while (itr.hasNext())
    {
      if (local_first_key != null)
      {
        break;
      }
      final KVSNode temp_node = my_virtual_nodes.get(itr.next());
      local_first_key = ((AbstractKVSNode) temp_node).findFirstKey();
    }
    while (itr.hasNext())
    {
      final KVSNode temp_node = my_virtual_nodes.get(itr.next());
      final String node_first_key = ((AbstractKVSNode) temp_node).findFirstKey();
      // First local check.
      if ((node_first_key != null) && (node_first_key.compareTo(local_first_key) < 0))
      {
        local_first_key = node_first_key;
      }
    }
    if (local_first_key != null && the_ring_first_key != null &&
        the_ring_first_key.compareTo(local_first_key) < 0)
    {

      local_first_key = the_ring_first_key;
    }
    int next_successor;
    if (my_successors.size() > 0)
    {
      next_successor = ((LinkedList<Integer>) my_successors).getFirst();
    }
    else
    {
      next_successor = getNodeId();
    }
    final RemoteMessage f_message =
        new RemoteMessage(the_final_dest, -1, next_successor, true, QueryType.FIRST_KEY,
                          the_msg_id, local_first_key, null);
    final int next_hop = getRouteTracker().getNextHop(next_successor, getNodeId());
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
  public String getLastKey() throws RemoteException
  {
    final int msg_id = getMessageId();
    getSentMessageTable().put(msg_id, new Result(Thread.currentThread()));
    calculateLastKeyThenSendOut(null, msg_id, getNodeId());
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
    String result = null;
    if (getSentMessageTable().get(msg_id).isResultArrived())
    {
      // check sent message table to get the result.
      final String ring_last_key = (String) getSentMessageTable().get(msg_id).getResult();
      result = ring_last_key;
      LogWriter.logLastKey(getNodeId(), result);
      getSentMessageTable().get(msg_id).setResultPassed(true);
    }
    return result;
  }

  /**
   * This method calculates a local last key, then compare with the last key in
   * the ring which has been calculated, then pass to next successor.
   * 
   * @param the_ring_last_key a calculated value in its predecessors.
   * @param the_msg_id a message id having the result of this calculation.
   * @param the_final_dest final destination of this calculation, final node.
   */
  public void calculateLastKeyThenSendOut(final String the_ring_last_key,
                                          final int the_msg_id, final int the_final_dest)
  {
    final Iterator<Integer> itr = my_virtual_nodes.keySet().iterator();
    String local_last_key = findLastKey();
    while (itr.hasNext())
    {
      if (local_last_key != null)
      {
        break;
      }
      final KVSNode temp_node = my_virtual_nodes.get(itr.next());
      local_last_key = ((AbstractKVSNode) temp_node).findLastKey();
    }
    while (itr.hasNext())
    {
      final KVSNode temp_node = my_virtual_nodes.get(itr.next());
      final String node_last_key = ((AbstractKVSNode) temp_node).findLastKey();
      // First local check.
      if ((node_last_key != null) && (local_last_key.compareTo(node_last_key) < 0))
      {
        local_last_key = node_last_key;
      }
    }
    // second, global check.
    if ((local_last_key != null) && (the_ring_last_key != null) &&
        (local_last_key.compareTo(the_ring_last_key) < 0))
    {
      local_last_key = the_ring_last_key;
    }
    int next_successor;
    // decide a next actual node it will pass the calculated value.
    if (my_successors.size() > 0)
    {
      next_successor = ((LinkedList<Integer>) my_successors).getFirst();
    }
    else
    {
      next_successor = getNodeId();
    }
    final RemoteMessage f_message =
        new RemoteMessage(the_final_dest, -1, next_successor, true, QueryType.LAST_KEY,
                          the_msg_id, local_last_key, null);
    final int next_hop = getRouteTracker().getNextHop(next_successor, getNodeId());
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
  public int getTotalNumberOfKeys() throws RemoteException
  {
    final int msg_id = getMessageId();
    getSentMessageTable().put(msg_id, new Result(Thread.currentThread()));
    calculateTotalNumberOfKeysThenSendOut("0", msg_id, getNodeId());
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
    int result = -1;
    if (getSentMessageTable().get(msg_id).isResultArrived())
    {
      // add the number of keys which has been added between sending out the
      // previous local total and getting the result.
      // check sent message table to get the result.
      final String ring_total_key = (String) getSentMessageTable().get(msg_id).getResult();
      result = Integer.valueOf(ring_total_key);
      LogWriter.logNumofKey(getNodeId(), result);
      getSentMessageTable().get(msg_id).setResultPassed(true);
    }
    return result;
  }

  /**
   * This method calculates total number of keys in the ring. It calculate the
   * values in local (including its virtual nodes), then compare the value with
   * the value has been calculated in its predecessors.
   * 
   * @param the_ring_total_key a total number of keys has been calculated in the
   *          predecessor nodes..
   * @param the_msg_id a message id having the result of this calculation.
   * @param the_final_dest final destination of this calculation, final node.
   */
  public void calculateTotalNumberOfKeysThenSendOut(final String the_ring_total_key,
                                                    final int the_msg_id,
                                                    final int the_final_dest)
  {
    final Iterator<Integer> itr = my_virtual_nodes.keySet().iterator();
    int local_total_key = findTotalKeys();
    while (itr.hasNext())
    {
      final KVSNode temp_node = my_virtual_nodes.get(itr.next());
      local_total_key += ((AbstractKVSNode) temp_node).findTotalKeys();
    }
    if (Validator.isInteger(the_ring_total_key))
    {
      local_total_key += Integer.valueOf(the_ring_total_key);
    }
    int next_successor;
    if (my_successors.size() > 0)
    {
      next_successor = ((LinkedList<Integer>) my_successors).getFirst();
    }
    else
    {
      next_successor = getNodeId();
    }
    final RemoteMessage f_message =
        new RemoteMessage(the_final_dest, -1, next_successor, true, QueryType.TOTAL_KEY,
                          the_msg_id, String.valueOf(local_total_key), null);
    final int next_hop = getRouteTracker().getNextHop(next_successor, getNodeId());
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
   * Main method to start an actual node. It gets only one command line
   * parameter, the location of a config.properties file.
   * 
   * @param the_strings is not used.
   */
  public static void main(final String... the_strings)
  {
    final Properties properties = new Properties();
    try
    {
      // Load the properties file.
      properties.load(new FileInputStream(the_strings[0]));
      final KVSNode actual_node = new ActualKVSNodeImpl(properties);
      ((ActualKVSNodeImpl) actual_node).runNode();
    }
    catch (final IOException e)
    {
      System.err.println("You should have a legal properties file as a "
                         + "commendline parameter for this application");
    }
  }

}
