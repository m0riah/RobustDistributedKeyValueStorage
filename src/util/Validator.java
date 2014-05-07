/*
 * Spring 2013 TCSS 558 - Applied Distributed Computing Institute of Technology,
 * UW Tacoma Written by Sky Moon
 */

package util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This static class is to validate general things: integer, boolean, float,
 * long, FQDN, email, host name and Ip address using Regex (regular expression
 * package in JAVA). Most of the codes are borrowed from the Internet:
 * http://pgadekar.blogspot.com/2011/01/validating-fqdn-url-e-mail-hostname.html
 * 
 * @author sky
 * @version 4/15/2013
 */
public final class Validator
{

  /**
   * The minimum acceptable port number.
   */
  private static final int MIN_PORT = 1025;

  /**
   * The maximum acceptable port number.
   */
  private static final int MAX_PORT = 65535;

  /**
   * Valid email address regular expression.
   */
  private static final Pattern EMAIL_PATTERN = Pattern
      .compile("^[\\w\\-]([\\.\\w])" + "+[\\w]+@([\\w\\-]+\\.)+[A-Z]{2,4}$",
               Pattern.CASE_INSENSITIVE);

  /**
   * Valid FQDN regular expression.
   */
  private static final Pattern FQDN_PATTERN = Pattern
      .compile("(?=^.{1,254}$)"
                   + "(^(?:(?!\\d+\\.|-)[a-zA-Z0-9_\\-]{1,63}(?<!-)\\.?)+(?:[a-zA-Z]{2,})$)",
               Pattern.CASE_INSENSITIVE);

  /**
   * Valid IP address regular expression. Borrowed from
   * http://www.mkyong.com/regular-expressions/
   * how-to-validate-ip-address-with-regular-expression/.
   */
  private static final Pattern IP_PATTERN =
      Pattern
          .compile("^([01]?\\d\\d?"
                   + "|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]"
                   + "\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

  /**
   * Valid host name regular expression. Borrowed from
   */
  private static final Pattern HOST_PATTERN = Pattern
      .compile("^(?=.{1,255}$)" + "[0-9A-Za-z](?:(?:[0-9A-Za-z]|-){0,61}[0-9A-Za-z])?"
                   + "(?:\\.[0-9A-Za-z](?:(?:[0-9A-Za-z]|-){0,61}[0-9A-Za-z])?)*\\.?$",
               Pattern.CASE_INSENSITIVE);

  /**
   * Unused constructor.
   */
  private Validator()
  {
  }

  /**
   * If the input String is integer, returns true.
   * 
   * @param the_int integer value in String.
   * @return true if the input is integer, false else.
   */
  public static boolean isInteger(final String the_int)
  {
    boolean result = false;
    try
    {
      new Integer(the_int);
      result = true;
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

  /**
   * If the input String is prot number, returns true.
   * 
   * @param the_port_number integer value in String.
   * @return true if the input is integer, false else.
   */
  public static boolean isPortNumber(final String the_port_number)
  {
    boolean result = false;
    try
    {
      if (Integer.valueOf(the_port_number) >= MIN_PORT &&
          MAX_PORT >= Integer.valueOf(the_port_number))
      {
        result = true;
      }
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

  /**
   * If the input String is boolean, returns true.
   * 
   * @param the_bool a boolean value in String.
   * @return true if the input is boolean, false else.
   */
  public static boolean isBoolean(final String the_bool)
  {
    boolean result = false;
    try
    {
      new Boolean(the_bool);
      result = true;
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

  /**
   * If the input String is float, return true.
   * 
   * @param the_float a boolean value in String.
   * @return true if the input is float, false else.
   */
  public static boolean isFloat(final String the_float)
  {
    boolean result = false;
    try
    {
      new Float(the_float);
      result = true;
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

  /**
   * If the input String is long, return true.
   * 
   * @param the_long a long value in String.
   * @return true if the input is long, false else.
   */
  public static boolean isLong(final String the_long)
  {
    boolean result = false;
    try
    {
      new Long(the_long);
      result = true;
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

  /**
   * If the input String is valid FQDN, return true.
   * 
   * @param the_fqdn FQDN string.
   * @return true if the input is FQDN, false else.
   */
  public static boolean isFQDN(final String the_fqdn)
  {
    boolean result = false;
    try
    {
      final Matcher matcher = FQDN_PATTERN.matcher(the_fqdn);
      result = matcher.matches();
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;

  }

  /**
   * If the input String is valid email address, return true.
   * 
   * @param the_email email address inn String.
   * @return true if the input is email address, false else.
   */
  public static boolean isEmailAddr(final String the_email)
  {
    boolean result = false;
    try
    {
      final Matcher matcher = EMAIL_PATTERN.matcher(the_email);
      result = matcher.matches();
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

  /**
   * Returns true if it is a valid host name, false if not.
   * 
   * @param the_hostname a string of the host name.
   * @return true if the host name is valid, false else.
   */
  public static boolean isHost(final String the_hostname)
  {
    boolean result = false;
    try
    {
      final Matcher matcher = HOST_PATTERN.matcher(the_hostname);
      result = matcher.matches();
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

  /**
   * Returns true if it is a valid ip address, false if not.
   * 
   * @param the_ip a string of the ip address.
   * @return true if the ip address is true, false else.
   */
  public static boolean isIP(final String the_ip)
  {
    boolean result = false;
    try
    {
      final Matcher matcher = IP_PATTERN.matcher(the_ip);
      result = matcher.matches();
    }
    catch (final NumberFormatException e)
    {
      System.out.println("");
    }
    return result;
  }

}
