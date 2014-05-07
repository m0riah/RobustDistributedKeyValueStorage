/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing
 * Institute of Technology, UW Tacoma
 * Written by Daniel M. Zimmerman
 */

package util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * A very simple logging class that prints to either standard error or
 * standard output, while adding timestamps to the messages.
 * 
 * @author Daniel M. Zimmerman
 * @version Spring 2013
 */
public final class Log
{
  /**
   * The date formatter to be used.
   */
  private static final SimpleDateFormat DATE = 
    new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS", Locale.US);
  
  /**
   * The separator between the date and the message.
   */
  private static final String SEPARATOR = ": ";
  
  /**
   * A constructor to prevent instantiation.
   */
  private Log()
  {
    // do nothing
  }
  
  /**
   * Print a log message to standard output.
   * 
   * @param the_message The log message.
   */
  public static synchronized void out(final String the_message)
  {
    System.out.println(DATE.format(new Date()) + SEPARATOR + the_message);
  }
  
  /**
   * Print a log message to standard error.
   * 
   * @param the_message The log message.
   */
  public static synchronized void err(final String the_message)
  {
    System.err.println(DATE.format(new Date()) + SEPARATOR + the_message);
  }
}
