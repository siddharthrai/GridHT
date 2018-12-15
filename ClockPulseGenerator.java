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
public class ClockPulseGenerator extends GridSimCore{
    private int myid;
    private int nodeid;
    private boolean isrunning;
    private long pulsecount;
    /** Creates a new instance of ClockPulseGenerator */
    public ClockPulseGenerator(int nodeid_) throws Exception {
        super("clockgenerator");
        nodeid=nodeid_; 
        isrunning=true;
        pulsecount=0;
    }
    
    public void body()
    {
        double uptime=Sim_system.clock();
        double duration=0.0;
        int delay;
        Random rand=new Random();
        Sim_event ev=new Sim_event();
        ClockPulse pulse =new ClockPulse(0);  
        delay=rand.nextInt(10);
        try{
            
        
        while(pulse.getPulseCount()<2*3600)
        {
            //this.sleep(100);
            this.sim_pause(1.0);
            //if(pulsecount%2==0)
            {
                //System.out.println("clockpulse send at "+Sim_system.clock());
               this.send(nodeid,GridMonitorTags.SCHEDULE_NOW,GridMonitorTags.CLOCK_PULSE,new GridMonitorIO(this.myid,this.nodeid,new ClockPulse(pulse.getPulseCount())));
            }
            duration=Sim_system.clock()-uptime; 
            pulse.increasePulseCount();
            pulsecount=pulse.getPulseCount();
            
        }
        isrunning=false;
        }catch(Exception e){
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
    
    
}
