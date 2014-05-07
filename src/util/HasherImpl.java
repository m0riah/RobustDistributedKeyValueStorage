/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sean
 */

package util;

/**
 * A implementation of the Hasher interface.
 * 
 * @author sean
 * @version 5/3/2013
 */
public class HasherImpl implements Hasher
{

  /**
   * Total node number in the node.
   */
  private int my_total_node_number;

  /**
   * Constructor for Hasher.
   * 
   * @param the_total_node_number the total number of nodes in the ring.
   */
  public HasherImpl(final int the_total_node_number)
  {
    my_total_node_number = the_total_node_number;
  }

  /**
   * Getter for the my_total_node_number.
   * 
   * @return the total number of nodes in the ring.
   */
  public int getTotalNodeNumber()
  {
    return my_total_node_number;
  }

  /**
   * Set the total number of nodes in the ring.
   * 
   * @param the_total_node_number the total number of nodes in the ring.
   */
  public void setTotalNodeNumber(final int the_total_node_number)
  {
    my_total_node_number = the_total_node_number;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getValue(final String the_message)
  {
    final int hash_value = the_message.hashCode() + 1;
    return Math.abs(hash_value) % my_total_node_number;
  }

}
