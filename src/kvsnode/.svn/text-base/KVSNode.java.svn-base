/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sky Moon
 */

package kvsnode;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This is a node which is represented as a stub in RMI registry.
 * 
 * - related instruction in the instruction document -
 * 
 * Storage and retrieval of values for specific keys in the system (that is,
 * communication between the system and the outside world) is carried out by
 * means of RMI calls; the goal here is that if a client can find any individual
 * node in the system and make an RMI call on it, it can store or retrieve the
 * data for any key supported by the system. This means that it is possible that
 * multiple stores to the same key will occur "simultaneously"; in such
 * situations, as long as one of the stored values ends up stored, your system
 * will be considered to work correctly. Note that such RMI calls will likely
 * result in asynchronous communication occurring within the system (to retrieve
 * the requested value, or to store a new value) before a response can be
 * generated; this may require some thread synchronization. ddd
 * 
 * @author sky
 * @version 5/13/2013
 */
public interface KVSNode extends Remote
{

  /** 
   * A frequently used string in the program.
   */
  String BACKUP_LIST = "backuplist";
  
  /**
   * The name of Remote Registry (fake) inside the real registry.
   */
  String REMOTE_REGISTRY = "RemoteRegistry";

  /**
   * A length of the BlockedQueue to put the clients in the queue until the
   * session is available.
   */
  int QUEUE_LENGTH_FOR_CHANNELS = 500;

  /**
   * A length of TransferQueue which converges the queues for each channels.
   */
  int QUEUE_LENGTH_FOR_MAIN_TRANSFER_CHANNEL = 1000;

  /**
   * A number of thread in each queue to process the messages.
   */
  int POOL_SIZE_FOR_PORT_QUEUE = 1;

  /**
   * Alive time of the idle thread in seconds.
   */
  int ALIVE_SEC_OF_IDLE_THREAD = 10;

  /**
   * A time Client can wait for the query.
   */
  int CLIENT_QUERY_WAIT_TIME = 8000;

  /**
   * Message for the request failed.
   */
  String REQUEST_FAIL = "Request failed";

  /**
   * Send a String query from a client to the node.
   * 
   * @param the_message a string message.
   * @return a Future object which will have the value of retrieve query.
   * @throws RemoteException if there is a problem completing the method call.
   */
  String retrieveData(String the_message) throws RemoteException;

  /**
   * Store data in a ring by making RMI calls on any node in the ring.
   * 
   * @param the_message store a key from a client.
   * @return a Future object which will have the result of store query.
   * @throws RemoteException if there is a problem completing the method call.
   */
  String storeData(String the_message) throws RemoteException;

  /**
   * Get a first key in the ring. null value will be returned if the process
   * failed.
   * 
   * @return a first key.
   * @throws RemoteException if there is a problem completing the method call.
   */
  String getFirstKey() throws RemoteException;

  /**
   * Get a last key in the ring. null value will be returned if the process
   * failed.
   * 
   * @return a last key.
   * @throws RemoteException if there is a problem completing the method call.
   */
  String getLastKey() throws RemoteException;

  /**
   * Get a total number of keys in the ring. If it fails, returns -1.
   * 
   * @return a total number of keys.
   * @throws RemoteException if there is a problem completing the method call.
   */
  int getTotalNumberOfKeys() throws RemoteException;

  /**
   * Get a list of actual nodes in the ring. null value will be returned if the
   * process failed.
   * 
   * @return a lust if actual node in the ring.
   * @throws RemoteException if there is a problem completing the method call.
   */
  List<String> getActualNodeList() throws RemoteException;

  /**
   * This method is to send a message to the node having this method from
   * another node.
   * 
   * - related instruction in the instruction document -
   * 
   * The keys in the system are Strings, and the values are arbitrary
   * Serializable objects (so that they can be sent/received using RMI). Each
   * key is mapped (hashed) to one (and only one) of the 2^m nodes; more than
   * one key may (and almost certainly will) be mapped to the same node, as
   * there are far more possible Strings than nodes. You can devise any method
   * you like to perform the mapping; for the purposes of these assignments, the
   * "quality" of your hashing algorithm is not important. Ideally, it would be
   * uniform: 1/2^m of the entire possible key space would be mapped to each
   * node.
   * 
   * @param the_source_node_id a node id from departure
   * @param the_dest_port_id a port number the message should be delivered.
   * @param the_message a message to process.
   * @throws RemoteException if there is a problem completing the method call.
   */
  void sendMessage(int the_source_node_id, int the_dest_port_id, RemoteMessage the_message)
    throws RemoteException;

  /**
   * When this method is invoked by a new actual node which tries to join in the
   * ring, pass all the data stored in the virtual nodes which will be taken
   * over by the actual node. The invocation on a virtual node only works. If
   * the method is invoked on an actual node, it returns "declined" in (0,0)
   * position of the 2D array.
   * 
   * This method is supposed to be invoked twice.
   * 
   * First time, a joining actual node invoke this method with the_process_code
   * "first", then an existing actual node managing the invoked virtual node
   * halt all the message and client query process, then give all data to set up
   * the actual node and its virtual nodes(an auto generated process code, list
   * of successor actual nodes and keys stored in the existing virtual nodes).
   * 
   * Second time, a joining node finish all initialization (sets the successor
   * actual node list, simulates virtual nodes with data given) and rebind all
   * itself and its virtual nodes then invoke this method again with the
   * returned process code, then the actual node managing the previous virtual
   * nodes resumes all the halt message process and client queries. Then
   * terminates the virtual nodes.
   * 
   * - related instruction in the instruction document -
   * 
   * A new actual node can join the system at any time when the system has less
   * than 2^m actual nodes. When a new actual node joins the system, it takes an
   * available number (currently being simulated by another actual node) and
   * informs some other nodes in the system (by sending messages appropriately).
   * When this happens, the nodes rebalance such that they all simulate
   * appropriate sets of nodes and all store appropriate key/value pairs. Such
   * rebalancing should be as localized as possible; adding one actual node to a
   * 1024-node ring with a nontrivial number of existing actual nodes should not
   * affect every other actual node on the ring. You can devise any method you
   * like to perform this rebalancing. For the purposes of these assignments you
   * may assume that, when an actual node is added to the system, you always
   * have sufficient time to complete your rebalancing before another actual
   * node is added to the system.
   * 
   * @throws RemoteException if there is a problem completing the method call.
   * @return 2D array of String having Data stored in the virtual nodes which
   *         the new actual node take over.
   */
  Map<String, Collection<String>> joinToRing() throws RemoteException;

  /**
   * If this method is invoked by a client, if the node invoked is a virtual
   * node, it returns "Not Allowed" If it is an actual node, it returns remote
   * exception.
   * 
   * - related instruction in the instruction document -
   * 
   * An actual node can leave the system at any time, either in an orderly
   * fashion (by notifying other nodes as appropriate) or by "crashing"
   * (becoming unresponsive). When this happens, the other actual nodes must
   * eventually detect it and rebalance. For the purposes of these assignments
   * you may assume that an actual node leaving the ring in an orderly fashion
   * always stays in the ring long enough for its data to be reallocated to
   * other actual nodes; that is, it does not leave the ring until it knows
   * leaving is "safe". You may not assume anything about how many actual nodes
   * leave the system, when they leave, or how they leave.
   * 
   * @throws RemoteException if there is a problem completing the method call.
   * @return "Not Allowed" if the node is a virtual node.
   */
  String invokeNodeToLeave() throws RemoteException;

  /**
   * Returns true if it is an actual node, false if not.
   * 
   * @throws RemoteException if there is a problem completing the method call.
   * @return true if it is an actual node, false if not.
   */
  boolean isActualNode() throws RemoteException;

  /**
   * To check whether this node is active or not. This is only for checking
   * whether a new actual nodes which is supposed to take the position current
   * node occupying in RMI registry take the position or not.
   * 
   * For example, an leaving actual node sets its active token to false, then
   * sent out all the data to another actual node to let the node take the
   * position, the actual node keep looking up its own ID from the registry to
   * invoke this method. If the return value is false, the rebinding has not
   * happened. If it is true, the rebinding happens, so the leaving actual node
   * can leave after making empty the message queues in it's and its virtual
   * nodes.
   * 
   * @return true if it is active, false if it is not active (this node is
   *         leaving soon).
   * @throws RemoteException if there is a problem completing the method call.
   */
  boolean isActive() throws RemoteException;

}
