/*
 * GridMonitorBootNode.java
 *
 * Created on January 1, 2003, 12:58 AM
 *
 * @author Siddharth Rai
 *
 */

package gridmonitor;

import gridsim.*;
import eduni.simjava.*;

public class GridMonitorBootNode extends GridSimCore
{
  int nodeid;

  /** Creates a new instance of GridMonitorBootNode */
  public GridMonitorBootNode(String name) throws Exception
  {
    super(name);
    nodeid = this.get_id();
  }

  public void body()
  {
    Sim_event ev = new Sim_event();

    while (Sim_system.running())
    {
      super.sim_get_next(ev);

      System.out.println("received event from entity ID:" + ev.get_src());
    }
  }
}
