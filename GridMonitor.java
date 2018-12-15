/*
 * GridMonitor.java
 *
 * Created on January 1, 2003, 1:13 AM
 *
 * @author Siddharth Rai
 */

package gridmonitor;

import eduni.simjava.*;
import java.security.*;
import java.util.*;

public class GridMonitor 
{
  public static final int NODE_COUNT=1024;   

  private GridMonitorAdmin admin;

  private static Calendar calendar;

  private static Date simulationstartdate;

  /** Creates a new instance of GridMonitor */
  public GridMonitor() 
  {        
    init();
  }

  /*
   * Initializes simulation and instantiates the administrator node
   *
   */

  private void init()
  {
    try
    {
      Sim_system.initialise();

      admin    = new GridMonitorAdmin("admin");
      calendar = Calendar.getInstance();

      simulationstartdate = calendar.getTime();
    }
    catch(Exception e)
    {
    }
  }

  /*
   * Simulation start
   */

  public void startsimulation()
  {
    Sim_system.run();
  }

  /*
   * Gets the administrator node id.
   */

  public int getAdminId()
  {
    return admin.get_id();
  }

  public static Calendar getSimulationCalendar()
  {
    // make a new copy
    Calendar clone = calendar;

    if (calendar != null) 
    {
      clone = (Calendar) calendar.clone();
    }

    return clone;
  }

  public static Date getSimulationStartDate()
  {
    // make a new copy
    Date clone = simulationstartdate;

    if (simulationstartdate != null) 
    {
      clone = (Date) simulationstartdate.clone();
    }

    return clone;
  }
}
