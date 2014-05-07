/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sky Moon
 */

package kvsnode;

import java.util.concurrent.ExecutorService;

/**
 * A thread to process messages in multiple channels. It passes messages from
 * each channel to the converging one channel to simulate one thread in each
 * node.
 * 
 * @author sky
 * @version 5/8/2013
 */
public class MessageProcessorForEachChannel implements Runnable
{

  /**
   * The converging queue for the node.
   */
  private final ExecutorService my_converging_queue;

  /**
   * A message.
   */
  private final RemoteMessage my_message;

  /**
   * A node processing this thread.
   */
  private final KVSNode my_node;

  /**
   * A port id.
   */
  private final int my_port_id;

  /**
   * Constructor.
   * 
   * @param the_converging_queue a queue to gather all the messages from
   *          multiple channels.
   * @param the_port_id a port id.
   * @param the_message a message.
   * @param the_node a node processing this thread.
   */
  public MessageProcessorForEachChannel(final ExecutorService the_converging_queue,
                                        final int the_port_id,
                                        final RemoteMessage the_message, 
                                        final KVSNode the_node)
  {
    my_converging_queue = the_converging_queue;
    my_message = the_message;
    my_port_id = the_port_id;
    my_node = the_node;
  }

  /**
   * Pass the message to the converging channel.
   */
  @Override
  public void run()
  {
    my_converging_queue.submit(new MessageProcessorForConvergingChannel(my_message,
                                                                        my_port_id, my_node));
  }

}
