/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sky Moon
 */

package kvsnode;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * POJO message format which is supposed to be sent from a node to another node.
 * 
 * @author sky
 * @version 5/8/2013
 */
@SuppressWarnings("serial")
public class RemoteMessage implements Serializable
{

  /**
   * A original departure node the message was first sent from.
   */
  private final int my_original_departure_node;

  /**
   * A node sending backup data to the final destination node.
   */
  private final int my_backup_requesting_node;

  /**
   * A final destination node the message should finally get to.
   */
  private final int my_final_destination_node;

  /**
   * If the message is a query, then it is false. If the message has a result
   * for a query, then it is true.
   */
  private final boolean my_return_value;

  /**
   * The type of a query from the client.
   */
  private final QueryType my_query_type;

  /**
   * A message id for the message. This is needed to match it to the message
   * table in the node.
   */
  private final int my_message_id;

  /**
   * The message sent from a node to another.
   */
  private final String my_message;

  /**
   * A data set for passing data from one actual node to the other.
   */
  private final Map<String, Collection<String>> my_dataset;

  /**
   * A constructor.
   * 
   * @param the_original_departure_node a node the message is originally from.
   * @param the_backup_requesting_node a node sending backup request.
   * @param the_final_destination_node a node the message should be finally get
   *          to.
   * @param the_return_value if the return_value is true, this has a result for
   *          the query.
   * @param the_query_type a type of the query from a client.
   * @param the_message_id a message id for the original departure node. This
   *          message id is only unique in a node.
   * @param the_message a message itself.
   * @param the_dataset a data set for passing data from one actual node to the other.
   */
  public RemoteMessage(final int the_original_departure_node,
                       final int the_backup_requesting_node,
                       final int the_final_destination_node, final boolean the_return_value,
                       final QueryType the_query_type, final int the_message_id,
                       final String the_message,
                       final Map<String, Collection<String>> the_dataset)
  {
    my_original_departure_node = the_original_departure_node;
    my_backup_requesting_node = the_backup_requesting_node;
    my_final_destination_node = the_final_destination_node;
    my_return_value = the_return_value;
    my_query_type = the_query_type;
    my_message_id = the_message_id;
    my_message = the_message;
    my_dataset = the_dataset;
  }

  /**
   * Returns an original departure node for the message.
   * 
   * @return an original departure node.
   */
  public int getOriginalDepartureNode()
  {
    return my_original_departure_node;
  }

  /**
   * Returns a node id sending the backup request.
   * 
   * @return a node id sending the backup request.
   */
  public int getBackupRequestingNode()
  {
    return my_backup_requesting_node;
  }

  /**
   * Returns a final destination node for the message.
   * 
   * @return a final destination node.
   */
  public int getFinalDestinationNode()
  {
    return my_final_destination_node;
  }

  /**
   * Returns true if this has a return value for a query. Returns false if this
   * has a query.
   * 
   * @return true for a result of a query, false for a query.
   */
  public boolean isReturnValue()
  {
    return my_return_value;
  }

  /**
   * Get a query type.
   * 
   * @return a type of the query.
   */
  public QueryType getQueryType()
  {
    return my_query_type;
  }

  /**
   * Returns a message id. This message id is only unique in each node in a
   * ring. It is not unique in a ring.
   * 
   * @return a message id.
   */
  public int getMessageId()
  {
    return my_message_id;
  }

  /**
   * Returns a message.
   * 
   * @return a message.
   */
  public String getMessage()
  {
    return my_message;
  }

  /**
   * Returns a data set.
   * 
   * @return a data set.
   */
  public Map<String, Collection<String>> getDataset()
  {
    return my_dataset;
  }

}
