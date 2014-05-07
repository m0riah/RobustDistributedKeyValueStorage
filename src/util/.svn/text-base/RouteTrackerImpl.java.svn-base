/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sean
 */

package util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * The implementation of the RouteTracker interface.
 * 
 * @author sean, Kai
 * @version 5/3/2013
 * 
 */
public class RouteTrackerImpl implements RouteTracker
{

  /**
   * m value of the ring.
   */
  private final int my_m;

  /**
   * The number of nodes in the ring: 2^m.
   */
  private final int my_number_of_nodes;

  /**
   * Constructor for this class.
   * 
   * @param the_m the total number of nodes in the ring.
   */
  public RouteTrackerImpl(final int the_m)
  {
    my_m = the_m;
    my_number_of_nodes = (int) Math.pow(2, the_m);
  }

  /**
   * Getter for the my_total_node_number.
   * 
   * @return the total number of nodes in the ring.
   */
  public int getM()
  {
    return my_m;
  }

  /**
   * Get the farthest node it can pass the message.
   * 
   * @param the_hash_value a hash value of the message to deliver the message to
   *          the node having the same node id as the hash value.
   * @param the_node_id the id number of the node
   * @return the farthest node it can pass the message.
   */
  @Override
  public int getNextHop(final int the_hash_value, final int the_node_id)
  {
    int node_to_hop;
    if (the_hash_value == the_node_id)
    {
      node_to_hop = the_node_id;
    }
    else
    {
      final List<Integer> connected_nodes = new LinkedList<>();
      for (int i = 0; i < my_m; i++)
      {
        connected_nodes.add(getAdditionInRing(the_node_id, (int) Math.pow(2, i)));
      }
      int distance;
      if (!connected_nodes.isEmpty())
      {
        final Iterator<Integer> itr = connected_nodes.iterator();
        node_to_hop = itr.next();
        distance = getDistanceBetweenNodes(the_hash_value, node_to_hop);
        int temp;
        while (itr.hasNext())
        {
          temp = itr.next();
          if (getDistanceBetweenNodes(the_hash_value, temp) < distance)
          {
            distance = getDistanceBetweenNodes(the_hash_value, temp);
            node_to_hop = temp;
          }
        }
      }
      else
      {
        node_to_hop = the_node_id;
      }
    }
    return node_to_hop;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getAdditionInRing(final int the_node_id, final int the_added_value)
  {
    int result;
    if ((the_node_id + the_added_value) >= my_number_of_nodes)
    {
      result = the_node_id + the_added_value - my_number_of_nodes;
    }
    else
    {
      result = the_node_id + the_added_value;
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getDistanceBetweenNodes(final int the_destination, final int the_departure)
  {
    int result;
    if (the_destination - the_departure < 0)
    {
      result = my_number_of_nodes + the_destination - the_departure;
    }
    else
    {
      result = the_destination - the_departure;
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int findIncomingChannel(final int the_target_node, final int the_distance)
  {
    if (the_target_node - the_distance >= 0)
    {
      return the_target_node - the_distance;
    }
    else
    {
      return my_number_of_nodes + the_target_node - the_distance;
    }
  }

  /**
   * {@inheritDoc}
   */
  public int getNextNodeInRing(final int the_node_id)
  {
    int result;
    if (the_node_id == my_number_of_nodes - 1)
    {
      result = 0;
    }
    else
    {
      result = the_node_id + 1;
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  public int getPrevNodeInRing(final int the_node_id)
  {
    int result;
    if (the_node_id == 0)
    {
      result = my_number_of_nodes - 1;
    }
    else
    {
      result = the_node_id - 1;
    }
    return result;
  }

}
