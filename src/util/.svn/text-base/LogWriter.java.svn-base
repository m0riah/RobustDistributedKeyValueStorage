/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sean, Kai
 */

package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import kvsnode.QueryType;

/**
 * This class is to write any log message in a file. Every actual node logs its
 * activity (startup information including its ID number and the m for the
 * system, messages sent and received, values stored and retrieved, etc.) to a
 * log file with a reasonable name (e.g., "log-ID.txt" where "ID" is the node's
 * ID number).
 * 
 * @author sean, Kai
 * @version 5/4/2013
 */
public final class LogWriter
{
  /**
   * Platform independent new line.
   */
  private static final String NEW_LINE = System.getProperty("line.separator");

  /**
   * Platform independent file separator.
   */
  private static final String SEPARATOR = System.getProperty("file.separator");

  /**
   * A constructor to prevent instantiation.
   */
  private LogWriter()
  {
    // do nothing
  }

  /**
   * Log startup information for a node. Should be called every time a node is
   * created.
   * 
   * @param the_node_id the id of the startup node
   * @param the_m_value the m value
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logStartup(final int the_node_id, final int the_m_value)
  {
    final Double total_nodes = Math.pow(2, the_m_value);
    final String log_msg =
        "Startup Information:" + NEW_LINE + "Node ID: " + the_node_id + NEW_LINE +
            "\"M\" value: " + the_m_value + NEW_LINE + "Total nodes: " +
            total_nodes.intValue();
    final boolean write_startup = writeToFile(the_node_id, log_msg);
    return write_startup && logNodeChannel(the_node_id, the_m_value);
  }

  /**
   * Log virtual nodes managed by this node. Only valid for actual nodes.
   * 
   * @param the_node_id the id of the node
   * @param the_node_list the list of virtual node ids
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logVirtualNodes(final int the_node_id,
                                                     final List<Integer> the_node_list)
  {
    String log_msg = "Virtual nodes managed by node " + the_node_id + ": ";
    if (the_node_list.isEmpty())
    {
      log_msg += "None";
    }
    else
    {
      for (int vnode : the_node_list)
      {
        log_msg += Integer.toString(vnode) + " ";
      }
    }
    return writeToFile(the_node_id, log_msg);
  }

  /**
   * Log the nodes that are able to communicate with the node in question.
   * 
   * @param the_node_id the id of the startup node
   * @param the_m_value the m value
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logNodeChannel(final int the_node_id,
                                                    final int the_m_value)
  {
    String log_msg = "Nodes with open channels to this node: ";
    final Double total_nodes = Math.pow(2, the_m_value);
    for (int k = 0; k < the_m_value; k++)
    {
      Double value = the_node_id + (Math.pow(2, k) % total_nodes);
      if (value > total_nodes)
      {
        value = the_node_id - (Math.pow(2, k) % total_nodes);
      }
      log_msg += value.intValue() + ", ";
    }
    return writeToFile(the_node_id, log_msg);
  }

  /**
   * Log successor information for a node.
   * 
   * @param the_node_id the id of the startup node
   * @param the_successors a string of all successors
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logSuccessor(final int the_node_id,
                                                  final List<Integer> the_successors)
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("Successor actual nodes of this node " + the_node_id + ": ");
    if (the_successors.isEmpty())
    {
      sb.append("None");
    }
    else
    {
      for (int vnode : the_successors)
      {
        sb.append(vnode + " ");
      }
    }
    return writeToFile(the_node_id, sb.toString());
  }

  /**
   * Log backup data information.
   * 
   * Written by sky.
   * 
   * @param the_node_id an actual node having the backup data.
   * @param the_backup_node_ids the node IDs this the actual node backup for.
   * @return true if written successfully, false otherwise.
   */
  public static synchronized boolean logBackupData(final int the_node_id,
                                                   final List<Integer> the_backup_node_ids)
  {
    String log_msg = "List of Nodes which data is backed up in this node " + the_node_id + ": ";
    if (the_backup_node_ids.isEmpty())
    {
      log_msg += "None";
    }
    else
    {
      for (int vnode : the_backup_node_ids)
      {
        log_msg += Integer.toString(vnode) + " ";
      }
    }
    return writeToFile(the_node_id, log_msg);
  }

  /**
   * Log when/if the node safely leaves the ring.
   * 
   * @param the_node_id the id of the leaving node
   * @param the_new_node the id of the new actual node managing it
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logLeaving(final int the_node_id, final int the_new_node)
  {
    final String log_msg =
        "Node number " + the_node_id + " has left safely. " +
            "Changed into virtual node. Managed by actual node " + the_new_node;
    return writeToFile(the_node_id, log_msg);
  }

  /**
   * Log when/if a node attempts to leave ring, but is stopped.
   * 
   * @param the_node_id the id of the leaving node
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logAttemptedLeaving(final int the_node_id)
  {
    final String log_msg =
        "Node number " + the_node_id + " has attempted to leave. " + NEW_LINE +
            "However, it was the last actual node, so it was not allowed. NOONE LEAVES!!!";
    return writeToFile(the_node_id, log_msg);
  }

  /**
   * Log if/when the node is discovered to have crashed.
   * 
   * @param the_node_id the id of the crashing node
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logCrashed(final int the_node_id)
  {
    final String log_msg = "Node number " + the_node_id + " has crashed.";
    return writeToFile(the_node_id, log_msg);
  }

  /**
   * Log messages sent.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          sending the message.
   * @param the_sender_id the id of the sending node.
   * @param the_receiver_id the id of the receiving node.
   * @param the_message_type a type of the message.
   * @param the_message the message sent.
   * @return true if written successfully, false otherwise.
   */
  public static synchronized boolean logMsgSent(final int the_actual_node_id,
                                                final int the_sender_id,
                                                final int the_receiver_id,
                                                final QueryType the_message_type,
                                                final String the_message)
  {
    String message_type;
    if (the_message_type == QueryType.BACKUP)
    {
      message_type = "BACKUP";
    }
    else if (the_message_type == QueryType.RETRIEVE)
    {
      message_type = "RETRIEVE";
    }
    else if (the_message_type == QueryType.STORE)
    {
      message_type = "STORE";
    }
    else if (the_message_type == QueryType.FIRST_KEY)
    {
      message_type = "FIRST_KEY";
    }
    else if (the_message_type == QueryType.LAST_KEY)
    {
      message_type = "LAST_KEY";
    }
    else if (the_message_type == QueryType.TOTAL_KEY)
    {
      message_type = "TOTAL_KEY";
    }
    else if (the_message_type == QueryType.BACKUP_ALL_IN_PRED)
    {
      message_type = "BACKUP_ALL_IN_PRED";
    }
    else if (the_message_type == QueryType.ACTUAL_NODE)
    {
      message_type = "ACTUAL_NODE";
    }
    else if (the_message_type == QueryType.RESTORE)
    {
      message_type = "RESTORE";
    }
    else if (the_message_type == QueryType.BACKUP_ALL_IN_PRED)
    {
      message_type = "BACKUP_ALL_IN_PRED";
    }
    else if (the_message_type == QueryType.BACKUP_ALL_IN_SUCC)
    {
      message_type = "BACKUP_ALL_IN_SUCC";
    }
    else if (the_message_type == QueryType.BACKUP_REQUEST)
    {
      message_type = "BACKUP_REQUEST";
    }
    else
    {
      message_type = "VNODES_DATA";
    }
    final String log_msg =
        "Node " + the_sender_id + ": Sent a message (" + message_type + ", " + the_message +
            ") to a node " + the_receiver_id;
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log messages received.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          receiving the message.
   * @param the_receiver_id the id of the receiving node id.
   * @param the_sender_id a sending node id.
   * @param the_message_type a type of the message.
   * @param the_message the message received.
   * @return true if written successfully, false otherwise.
   */
  public static synchronized boolean logMsgReceived(final int the_actual_node_id,
                                                    final int the_receiver_id,
                                                    final int the_sender_id,
                                                    final QueryType the_message_type,
                                                    final String the_message)
  {
    String message_type;
    if (the_message_type == QueryType.BACKUP)
    {
      message_type = "BACKUP";
    }
    else if (the_message_type == QueryType.RETRIEVE)
    {
      message_type = "RETRIEVE";
    }
    else if (the_message_type == QueryType.STORE)
    {
      message_type = "STORE";
    }
    else if (the_message_type == QueryType.FIRST_KEY)
    {
      message_type = "FIRST_KEY";
    }
    else if (the_message_type == QueryType.LAST_KEY)
    {
      message_type = "LAST_KEY";
    }
    else if (the_message_type == QueryType.TOTAL_KEY)
    {
      message_type = "TOTAL_KEY";
    }
    else if (the_message_type == QueryType.BACKUP_ALL_IN_PRED)
    {
      message_type = "BACKUP_ALL_IN_PRED";
    }
    else if (the_message_type == QueryType.ACTUAL_NODE)
    {
      message_type = "ACTUAL_NODE";
    }
    else if (the_message_type == QueryType.RESTORE)
    {
      message_type = "RESTORE";
    }
    else if (the_message_type == QueryType.BACKUP_ALL_IN_PRED)
    {
      message_type = "BACKUP_ALL_IN_PRED";
    }
    else if (the_message_type == QueryType.BACKUP_ALL_IN_SUCC)
    {
      message_type = "BACKUP_ALL_IN_SUCC";
    }
    else if (the_message_type == QueryType.BACKUP_REQUEST)
    {
      message_type = "BACKUP_REQUEST";
    }
    else
    {
      message_type = "VNODES_DATA";
    }
    final String log_msg =
        "Node " + the_receiver_id + ": Received a message (" + message_type + ", " +
            the_message + ") from a node " + the_sender_id;
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log retrieveData query from a client.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          receiving the client query.
   * @param the_node_id the id of the receiving node id.
   * @param the_message the message received
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logClientRetrieveData(final int the_actual_node_id,
                                                           final int the_node_id,
                                                           final String the_message)
  {
    final String log_msg =
        "Node " + the_node_id + ": Got a retrieve data query (" + the_message +
            ") from a client.";
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log storeData query from a client.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          receiving the client query.
   * @param the_node_id the id of the receiving node id.
   * @param the_message the message received
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logClientStoreData(final int the_actual_node_id,
                                                        final int the_node_id,
                                                        final String the_message)
  {
    final String log_msg =
        "Node " + the_node_id + ": Got a store data query (" + the_message +
            ") from a client.";
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log values retrieved from the node.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_node_id the id of the node which stored value is being retrieved
   * @param the_message the message received
   * @param the_result the value of the retrieved value
   * @return true if written successfully, false otherwise
   */
  public static synchronized boolean logValuesRet(final int the_actual_node_id,
                                                  final int the_node_id,
                                                  final String the_message,
                                                  final String the_result)
  {
    final String log_msg =
        "Node " + the_node_id + ": Returned a result (" + the_result +
            ") for the retrieve data query (" + the_message + ").";
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log values stored in the node.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_node_id the id of the node storing value
   * @param the_message the message received
   * @param the_result the value stored
   * @return true if written successfully, false otherwise
   */
  public static boolean logValueStored(final int the_actual_node_id, final int the_node_id,
                                       final String the_message, final String the_result)
  {
    final String log_msg =
        "Node " + the_node_id + ": Stored a message (" + the_message + ") with result (" +
            the_result + ") then send it to predecessor to backup";
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log the first key, which the value is stored in the ring.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_result the value stored
   * @return true if written successfully, false otherwise
   */

  public static boolean logFirstKey(final int the_actual_node_id, final String the_result)
  {
    final String log_msg =
        "The first key for which a value: " + the_result + " in the ring is " +
            the_actual_node_id;
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log the last key, which the value is stored in the ring.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_result the value stored
   * @return true if written successfully, false otherwise
   */

  public static boolean logLastKey(final int the_actual_node_id, final String the_result)
  {
    final String log_msg =
        "The last key for which a value: " + the_result + " in the ring is " +
            the_actual_node_id;
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * the_number_of_key is number of keys for which values are stored in the
   * ring.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @return true if written successfully, false otherwise
   */
  public static boolean logNumofKey(final int the_actual_node_id, final int the_number_of_key)
  {
    final String log_msg =
        "The number of keys for which values are stored in the ring is " + the_number_of_key;
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * the_node_numbers is all the actual nodes in the ring.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_actual_list a list of actual nodes. 
   * @return true if written successfully, false otherwise.
   */
  public static boolean logListofActualNode(final int the_actual_node_id,
                                           final List<String> the_actual_list)
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("A list of actual nodes in the ring is : ");
    final Iterator<String> itr = the_actual_list.iterator();
    while (itr.hasNext())
    {
      sb.append(itr.next() +  " ");
    }
    return writeToFile(the_actual_node_id, sb.toString());
  }

  /**
   * Log values backed up in the node.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_node_id the id of the node storing value
   * @param the_message the message received
   * @param the_result the value stored
   * @return true if written successfully, false otherwise
   */
  public static boolean logValueBackedUp(final int the_actual_node_id, final int the_node_id,
                                         final String the_message, final String the_result)
  {
    final String log_msg =
        "Node " + the_node_id + ": Backup a message (" + the_message + ") with result (" +
            the_result + ")";
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log the backup data sent out.
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_reciever the id of the receiver of the backup data.
   * @param the_backup_list a backup list.
   * @return true if written successfully, false otherwise
   */
  public static boolean logBackedUpValueSent(final int the_actual_node_id,
                                             final int the_reciever,
                                             final List<Integer> the_backup_list)
  {
    final StringBuilder sb = new StringBuilder();
    final Iterator<Integer> itr = the_backup_list.iterator();
    sb.append("Sent out backup data to node " + the_reciever + " with (");
    while (itr.hasNext())
    {
      sb.append(itr.next() + " ");
    }
    sb.append(")");
    return writeToFile(the_actual_node_id, sb.toString());
  }

  /**
   * Log System failure detection (detected a node accidently left the ring
   * system).
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_node_id the id of the node storing value
   * @param the_failure_node a detected failure node.
   * @param the_message the message received
   * @return true if written successfully, false otherwise
   */
  public static boolean logSystemFailure(final int the_actual_node_id, final int the_node_id,
                                         final int the_failure_node, final String the_message)
  {
    final String log_msg =
        "Node " + the_node_id + ": Node failure detected on node " + the_failure_node +
            " while sending the message " + the_message;
    return writeToFile(the_actual_node_id, log_msg);
  }

  /**
   * Log System failure detection (detected a node accidently left the ring
   * system).
   * 
   * @param the_actual_node_id a actual node id which has the virtual node
   *          returning the result of client query.
   * @param the_restored_nodes a list of restored nodes.
   * @return true if written successfully, false otherwise
   */
  public static boolean logSystemRestored(final int the_actual_node_id,
                                          final List<Integer> the_restored_nodes)
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("Node " + the_actual_node_id + ": Restored the virtual nodes (");
    final Iterator<Integer> itr = the_restored_nodes.iterator();
    while (itr.hasNext())
    {
      sb.append(itr.next() + " ");
    }
    sb.append(")");
    return writeToFile(the_actual_node_id, sb.toString());
  }

  /**
   * Write log data to file.
   * 
   * @param the_node_id the id of the nodes log file to write to
   * @param the_log_data the data to write to file
   * @return true if written successfully, false otherwise
   */
  private static synchronized boolean writeToFile(final int the_node_id,
                                                  final String the_log_data)
  {
    final SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS", Locale.US);
    boolean written = true;
    try
    {
      // Make new directory
      final File dir = new File("Logs");
      dir.mkdir();

      // Time stamp log data.
      final String data = date.format(new Date()) + ": " + the_log_data + NEW_LINE;
      final File file =
          new File(dir.getAbsolutePath() + SEPARATOR + "log-" + the_node_id + ".txt");

      // Check if file exists, if not create new one. Assume unique node ID,
      // therefore unique filename.
      if (!file.exists())
      {
        file.createNewFile();
      }

      // Appends to file.
      final FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
      final BufferedWriter bw = new BufferedWriter(fw);
      bw.write(data);
      bw.close(); // Close file.
    }
    catch (final IOException e)
    {
      written = false;
      Log.err("Error writing to file.");
      e.printStackTrace();
    }
    return written;
  }

}
