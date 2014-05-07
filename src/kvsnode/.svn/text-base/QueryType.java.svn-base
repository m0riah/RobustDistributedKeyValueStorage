
package kvsnode;

/**
 * This is enum class to identify the type of query from the client.
 * 
 * @author sky
 * @version 5/20/2013
 */
public enum QueryType
{
  /**
   * Retrieve a String from the database.
   */
  RETRIEVE,

  /**
   * Store a String to the database.
   */
  STORE,

  /**
   * Request the actual node to update successors.
   */
  UPDATE_SUCCESSORS,
  
  /**
   * Simulate new virtual nodes with the data in the message.
   */
  VNODES_DATA,
  
  /**
   * Put this message to the backup storage, then send it to the original
   * departure node in the message.
   */
  BACKUP,
  
  /**
   * Update all Backup data in the message.
   */
  BACKUP_ALL_IN_PRED,
  
  /**
   * Update all backup data in successor.
   */
  BACKUP_ALL_IN_SUCC,
  
  /**
   * Backup data request.
   */
  BACKUP_REQUEST,
  
  /**
   * Restore data from node failure.
   */
  RESTORE,

  /**
   * To find a first key in lexicographic order.
   */
  FIRST_KEY,
  
  /**
   * To find a last key in lexicographic order.
   */
  LAST_KEY,

  /**
   * T find a total number of keys  in the ring.
   */
  TOTAL_KEY,
  
  /**
   * To find actual nodes in the ring.
   */
  ACTUAL_NODE,
  
}
