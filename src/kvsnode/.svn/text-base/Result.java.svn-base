
package kvsnode;

/**
 * The result for a client request which is maintained in the message table. The
 * ClientRequestProcessThread will keep checking the my_is_result_arrived field
 * to return the result to the client.
 * 
 * @author sky
 * @version 5/20/2013
 */
public class Result
{

  /**
   * If this object received the result value for the client query from other
   * node, it is true.
   */
  private boolean my_result_flag;

  /**
   * If the result is passed to the client, then change it to true.
   */
  private boolean my_result_passed;

  /**
   * This is to interrupt a sleeping thread in retrieveData or storeData methods
   * in AbstractKVSNode when the result come back from other node.
   */
  private final Thread my_thread;

  /**
   * Result value for the client.
   */
  private Object my_result;

  /**
   * Constructor with a thread parameter which is waiting until the result for
   * the client request come back.
   * 
   * @param the_thread a thread waiting the result coming back in retrieveData
   *          or storeData methods in AbstractKVSNode.
   */
  public Result(final Thread the_thread)
  {
    my_result_flag = false;
    my_result_passed = false;
    my_result = "";
    my_thread = the_thread;
  }

  /**
   * Getter for my_result_flag.
   * 
   * @return if the result is arrived, return true; else, false.
   */
  public boolean isResultArrived()
  {
    return my_result_flag;
  }

  /**
   * Setter my_result_flag.
   * 
   * @param the_result_flag true of false to indicate this object has a result
   *          or not.
   */
  public void setResultFlag(final boolean the_result_flag)
  {
    my_result_flag = the_result_flag;
  }

  /**
   * Getter for my_result.
   * 
   * @return the result for client query.
   */
  public Object getResult()
  {
    return my_result;
  }

  /**
   * Setter for my_result.
   * 
   * @param the_result the result for client query.
   */
  public void setResult(final Object the_result)
  {
    my_result = the_result;
  }

  /**
   * Check whether the result is passed to the client or not.
   * 
   * @return true, if it is not passed, false, if it is passed.
   */
  public boolean isResultPassed()
  {
    return my_result_passed;
  }

  /**
   * Set the boolean value my_result_passed which indicates whether the result
   * is passed to the client or not.
   * 
   * @param the_result_processed boolean value (true if it is passed, false if
   *          it is not passed.)
   */
  public void setResultPassed(final boolean the_result_processed)
  {
    my_result_passed = the_result_processed;
  }

  /**
   * Get a thread which might be sleeping in storeData or retrieveData in
   * AbstractKVSNode.
   * 
   * @return a thread.
   */
  public Thread getThread()
  {
    return my_thread;
  }

}
