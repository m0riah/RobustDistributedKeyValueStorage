/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing
 * Institute of Technology, UW Tacoma
 * Written by Sean, Sky Moon
 */

package util;

/**
 * This interface is to calculate the next hope to pass a message to the proper
 * node which should deal with the message.
 * 
 * - related instruction in the instruction document -
 * 
 * The nodes in the system are arranged (in terms of communication channels) as
 * a ring with chords. 2 Node i has channels to nodes i + 2^k for 0 <= k < m
 * (the same m as above), with all addition and subtraction done modulo 2^m ;
 * these are called node neighbors. For example, in an 8-node system, node 5
 * would have channels to nodes 6, 7, and 1; in a 128-node system, node 0 would
 * have channels to nodes 1, 2, 4, 8, 16, 32, and 64. The idea of setting up the
 * communication this way is that sending a message between any two nodes in a k
 * node system requires at most O(log k) message sends between nodes, while also
 * only requiring each node to keep track of O(log k) other nodes in the system.
 * 
 * @author sean, sky
 * @version 5/3/2013
 */
public interface RouteTracker
{
  /**
   * Get the farthest node it can pass the message.
   * 
   * @param the_hash_value a hash value of the message to deliver.
   * @param the_node_id the id number of the sender node
   * @return the farthest node it can pass the message.
   */
  int getNextHop(int the_hash_value, int the_node_id);
  
  /**
   * To calculate hop in the ring.
   * 
   * @param the_node_id a node id.
   * @param the_added_value a hop value adding to the node id in the ring.
   * @return a node id after hopping.
   */
  int getAdditionInRing(int the_node_id, int the_added_value);
  
  /**
   * To calculate the distance between two nodes.
   * 
   * @param the_destination a destination node.
   * @param the_departure a departure node.
   * @return a distance between the two nodes.
   */
  int getDistanceBetweenNodes(int the_destination, int the_departure);
  
  /**
   * To find the name of the port number, which is a node number indicating the
   * target node with a given distance.
   * 
   * @param the_target_node a target node to examine.
   * @param the_distance a distance from the target node.
   * @return the node number indicating the target node with the given distance.
   */
  int findIncomingChannel(int the_target_node, int the_distance);
  
  /**
   * This method is to calculate which is the next node in the ring. This method
   * is necessary to make it return the next node id when it gets to the 2^m - 1
   * node. This method returns 0 when it gets to the (2^m - 1)th node.
   * 
   * @param the_node_id current node id.
   * @return the next node id.
   */
  int getNextNodeInRing(int the_node_id);
  
  /**
   * Get the previous node in the ring.
   * 
   * @param the_node_id node to find previous node for
   * @return the previous node in ring
   */
  int getPrevNodeInRing(int the_node_id);
  
}
