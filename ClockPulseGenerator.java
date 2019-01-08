/*
 * ClockPulseGenerator.java
 *
 * Created on January 1, 2003, 5:06 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package gridmonitor;

/**
 *
 * @author Administrator
 */
import gridsim.*;
import eduni.simjava.*;
import java.util.*;
public class ClockPulseGenerator extends GridSimCore
{
    private int myid;
    private int nodeid;
    private boolean isrunning;
    private long pulsecount;
    private double latency;
    private ArrayList clocked_nodes;
    private int [] wait_time;
    
    /** Creates a new instance of ClockPulseGenerator */
    public ClockPulseGenerator(int nodeid_, long frequency_, ArrayList clocked_nodes_) throws Exception 
    {
        super("clockgenerator");
        
        nodeid      = nodeid_; 
        isrunning   = true;
        pulsecount  = 0;
        latency     = (1 / (double)frequency_) * 1000;
        clocked_nodes = (ArrayList)clocked_nodes_.clone();
        wait_time    = new int[clocked_nodes.size()];
        
        System.out.println("Latency in clock pulse = " + latency + " for frequency = " + frequency_);
    }
    
    public void body()
    {
        double uptime   = Sim_system.clock();
        double duration = 0.0;
                
        Random rand  = new Random();
        Sim_event ev = new Sim_event();
        
        ClockPulse pulse = new ClockPulse(0);  
                
        System.out.println("Clock pulse generator started");
        
        for (int node = 0; node < clocked_nodes.size(); node++)
        {
          wait_time[node] = 0;
        }
        
        // Consider system clock pulse is in pico-seconds
        // Generate clock pulse based on cycle time for a given component
        try
        {                  
          while (pulse.getPulseCount() < 1000)
          {
            //this.sleep(100);
            
            // Pause the entity for one clock to generate next clock pulse
            this.sim_pause(latency);
                     
            
            //if(pulsecount%2==0)
            for (int node = 0; node < clocked_nodes.size(); node++)
            {
               //System.out.println("clockpulse for node " + nodeid + " send at " + Sim_system.clock());
               if (wait_time[node] == 0)
               {
                this.send(node, GridMonitorTags.SCHEDULE_IMM, GridMonitorTags.CLOCK_PULSE, new GridMonitorIO(this.myid, node, new ClockPulse(pulse.getPulseCount())));
               }
               else if (wait_time[node] > 0)
               {                 
                 wait_time[node] = wait_time[node] - 1;
               }
               
               //System.out.println("Clocking node " + node);
               
               //System.out.println("Sending clock pulse " + pulsecount + " to node " + node);
            }              
            
            duration = Sim_system.clock() - uptime; 
            
            pulse.increasePulseCount();
            
            pulsecount = pulse.getPulseCount();
            
            System.out.println("Sending clock pulse " + pulsecount + " at " + Sim_system.clock());
          }
          
          isrunning= false;
          
          System.out.println("Clock genrator finished");
        }
        catch(Exception e)
        {
          System.out.println("Exception at line " + e.getStackTrace()[0].getLineNumber());
        }
    }
    
    public long getPulseCount()
    {
        return pulsecount;
    }
    public boolean isRunning()
    {
        return isrunning;
    }   
    
    public void setWaitTime(int nodeid, int clock_wait)
    {
      //System.out.println("Waittime for " + nodeid + " set to " + clock_wait);
      
      wait_time[nodeid] = clock_wait;
      
      return;
    }
}