/*
 * GridMonitorAdmin.java
 *
 * Created on January 1, 2003, 11:32 PM
 *
 * @author : Siddharth Rai
 *
 */

package gridmonitor;

import java.util.*;
import gridsim.*;
import eduni.simjava.*;

public class GridMonitorAdmin extends GridSimCore
{
  int nodeid;
  int bandwidth;
  int messagecount;
  int INDEX_DHT_NODES=6;
  int FEEDBACK_DHT_NODES=6;
  int count_nodes_system;
  int count_indexed_values;

  ArrayList indexnodes;
  ArrayList feedbacknodes;
  ArrayList brokerlist;    
  ArrayList grouplist;  //list of groups
  ArrayList queue;
  ArrayList differedqueue;    

  Group currentgroup;   //cuurrent group of nodes

  boolean isstable;
  boolean indexingcomplete;
  boolean tracingtraffic;

  double reqtime;
  double arrivaltime;

  ClockPulseGenerator clockpulsegenerator;

  /** Creates a new instance of GridMonitorAdmin */
  public GridMonitorAdmin(String name) throws Exception
  {
    super(name);

    indexnodes    = new ArrayList();
    feedbacknodes = new ArrayList();
    brokerlist    = new ArrayList();
    differedqueue = new ArrayList();        

    queue = new ArrayList();

    nodeid = this.get_id();

    clockpulsegenerator   = new ClockPulseGenerator(this.nodeid);
    count_nodes_system    = 0;
    count_indexed_values  = 0;

    isstable = false;

    indexingcomplete  = false;
    tracingtraffic    = false;
    currentgroup      = null;

    reqtime   = Sim_system.clock();
    grouplist = new ArrayList();

    bandwidth     = 30;
    messagecount  = 0;
    arrivaltime   = 0.0;

    //clockpulsegenerator=new ClockPulseGenerator(this.nodeid);
    System.out.println("Administartor node is up now !");
  }


  /*
   * Listens for incoming events and sends event to {@link processEvent} for
   * processing
   *
   */

  public void body() 
  {
    Sim_event ev;

    while (Sim_system.running()) 
    {
      ev = new Sim_event();

      this.getNextEvent(ev);

      if (ev.get_src() != -1) 
      {
        processEvent(ev);
      }

      ev = null;
    }
  }

  /*
   *  Processes incoming events
   *
   *  Parameters: 
   *    ev_  : sim event object
   *
   */

  private void processEvent(Sim_event ev_) 
  {
    int src;
    int dest;

    Object data;

    src   = ((GridMonitorIO)ev_.get_data()).getsrc();
    dest  = ((GridMonitorIO)ev_.get_data()).getdest();
    data  = ((GridMonitorIO)ev_.get_data()).getdata();

    Iterator i;

    switch(ev_.get_tag()) 
    {
      case GridMonitorTags.ROLE_GET:
        //System.out.println("admin obtained get role at "+Sim_system.clock());
        queue.add(ev_);

        if(currentgroup == null)
        {
          currentgroup = new Group(src);
          grouplist.add(currentgroup);
        }
        else
        {
          if (currentgroup.isFull() == true)
          {
            currentgroup = new Group(src);
            grouplist.add(currentgroup);
          }
        }     

        currentgroup.addMember(src);                    

        count_nodes_system = (count_nodes_system == 0) ? count_nodes_system + 1 : 
          count_nodes_system;

        if (count_nodes_system != 1) 
        {
          break;
        }

        //System.out.println("Source id:"+src+"Destination id:"+dest);
        
      case GridMonitorTags.JOIN_COMPLETE:
        if(queue.isEmpty() != true) 
        {
          count_nodes_system += 1;

          ev_   = (Sim_event)queue.remove(0);
          src   = ((GridMonitorIO)ev_.get_data()).getsrc();
          dest  = ((GridMonitorIO)ev_.get_data()).getdest();
          data  = ((GridMonitorIO)ev_.get_data()).getdata();

          if (indexnodes.size() < INDEX_DHT_NODES) 
          {
            indexnodes.add(((GridMonitorIO)ev_.get_data()).getsrc());
            if(indexnodes.size() == 1) 
            {
              send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.ROLE_INDEX_DHT_NODE, 
                  new GridMonitorIO(nodeid, src, null));
            } 
            else 
            {
              send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.ROLE_INDEX_DHT_NODE, 
                  new GridMonitorIO(nodeid, src, (Object)indexnodes.get(0)));
            }
          } 
          else 
          {
            if(feedbacknodes.size() < FEEDBACK_DHT_NODES) 
            {
              feedbacknodes.add(((GridMonitorIO)ev_.get_data()).getsrc());

              if (feedbacknodes.size() == 1) 
              {
                send(src,GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.ROLE_FEEDBACK_ADMIN, 
                    new GridMonitorIO(nodeid, src, null));
              } 
              else 
              {
                send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.ROLE_FEEDBACK_DHT_NODE, 
                    new GridMonitorIO(nodeid, src, (Object)feedbacknodes.get(0)));
              }
            } 
            else 
            {
              send(src,GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.ROLE_RESOURCE, 
                  new GridMonitorIO(nodeid, src, (Object)indexnodes.get(0)));
            }
          }
        }
        else 
        {
          // TODO: initiate processing deferred queue.
          
          //System.out.println("join completed..."+differedqueue.size());
          i = differedqueue.iterator();

          while(i.hasNext()) 
          {
            ev_ = (Sim_event)differedqueue.remove(0);

            if (ev_.get_tag() != GridMonitorTags.ADD_BROKER) 
            {
              processOtherEvents(ev_);
            } 
            else 
            {
              differedqueue.add(ev_);
            }

            // this.send(dest,10.0,GridMonitorTags.START_DHT_INDEXING,new GridMonitorIO(this.nodeid,dest,null));
          }

          while(differedqueue.isEmpty()!=true) 
          {
            ev_ = (Sim_event)differedqueue.remove(0);

            processOtherEvents(ev_);
          }

          isstable=true;
        }

        break;

      case GridMonitorTags.ADD_BROKER:
        processOtherEvents(ev_);
        break;

      default:
        if (isstable != true) 
        {
          differedqueue.add(ev_);
        } 
        else 
        {
          processOtherEvents(ev_);
        }

        break;
    }
  }

  public void processOtherEvents(Sim_event ev_) 
  {
    int src;
    int dest;

    Object data;

    Iterator i;

    src   = ((GridMonitorIO)ev_.get_data()).getsrc();
    dest  = ((GridMonitorIO)ev_.get_data()).getdest();
    data  = ((GridMonitorIO)ev_.get_data()).getdata();

    //System.out.println("****"+ev_.get_tag());
    switch(ev_.get_tag()) 
    {
      case GridMonitorTags.ADD_BROKER:
        if (isstable == true && indexingcomplete == true) 
        {
          System.out.println("Broker added with id " + src);
          brokerlist.add(data);

          this.send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.BROKER_ADDED, 
              new GridMonitorIO(nodeid, src, (Object)indexnodes.get(0)));
        } 
        else 
        {
          this.send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.RETRY, 
              new GridMonitorIO(nodeid, src, null));
        }

        break;

      case GridMonitorTags.GET_A_INDEX_NODE:
        //System.out.println("Request for an index:"+src);
        this.send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.A_INDEX_NODE, 
            new GridMonitorIO(this.nodeid, src, (Object)indexnodes.get(0)));
        break;

      case GridMonitorTags.GET_A_FEEDBACK_NODE:
        //System.out.println("Request for an index:"+src);
        this.send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.A_FEEDBACK_NODE, 
            new GridMonitorIO(this.nodeid, src, (Object)feedbacknodes.get(0)));
        break;

      case GridMonitorTags.COUNT:
        count_indexed_values += 1;
        //System.out.println("---"+count_indexed_values+"---");
        if (this.count_indexed_values == 160) 
        {
          indexingcomplete = true;
          i = indexnodes.iterator();

          while (i.hasNext()) 
          {
            dest = (Integer)(i.next());

            this.send(dest, 0.0, GridMonitorTags.GET_INDEX_SIZE, new GridMonitorIO(this.nodeid, dest, null));
          }

          //send memberlist to each group leader
          i = grouplist.iterator();

          while (i.hasNext())
          {
            currentgroup = (Group)i.next();
            src = currentgroup.getLeader();

            System.out.println("Leader is " + src);

            this.send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.GROUP, new GridMonitorIO(this.nodeid, src, currentgroup.getMember()));
          }

          src = (Integer)this.feedbacknodes.get(0);
          this.send(src, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.START, new GridMonitorIO(this.nodeid, src, null));

          i = indexnodes.iterator();

          while (i.hasNext()) 
          {
            dest = (Integer)(i.next());
            //this.send(dest,10.0,GridMonitorTags.PRINT,new GridMonitorIO(this.nodeid,dest,null));
          }
        }
        break;                

      case GridMonitorTags.SUCCESSOR:
        System.out.println("*******Response time "+(Sim_system.clock()-reqtime));

        tracingtraffic = false;
        break;

      case GridMonitorTags.PING:
        src = ((GridMonitorIO)ev_.get_data()).getsrc();
        this.sim_process(1.0);

        //if(pingrequest)
        //System.out.println("Received at "+Sim_system.clock());
        this.send(src, 0.0, GridMonitorTags.PONG, new GridMonitorIO(this.nodeid, src, null));
    }
  }

  private void serviceDemonTask(int pulsecount_)
  {        
    double val;
    byte   hashkey[];
    int    src;

    if (pulsecount_ % 6 == 0)
    {
      if (this.indexingcomplete == true && tracingtraffic == false)
      {
        val     = 0.80;
        hashkey = new byte[20];
        src     = (Integer)this.indexnodes.get(0);

        tracingtraffic = true;

        HashCode.computeConsistentHash(val,hashkey);

        reqtime = Sim_system.clock();
        // this.send(src,GridMonitorTags.SCHEDULE_NOW,GridMonitorTags.FIND_SUCCESSOR,new GridMonitorIO(this.nodeid,src,hashkey));
      }
    }
  }

  private void getNextEvent(Sim_event ev_)
  {
    boolean finish = false;

    ClockPulse pulse;

    if (messagecount == bandwidth)
    {
      //System.out.println("*****");
      //this.sim_process(1.0);  
      messagecount = 0;
    }

    while (!finish)
    {
      this.sim_get_next(ev_);        

      if(ev_.get_tag() == GridMonitorTags.CLOCK_PULSE && Sim_system.running())
      {            
        pulse = (ClockPulse)(((GridMonitorIO)ev_.get_data()).getdata());                           
        //serviceDemonTask(pulse.getPulseCount());               

        //System.out.println("Clock pulse:"+pulse.getPulseCount()+" received at "+this.nodeid);
      }
      else
      {       
        if (arrivaltime == Sim_system.clock())
        {  
          messagecount++;                                   
        }
        else
        {
          arrivaltime   = Sim_system.clock();
          messagecount  = 1;
        } 

        finish = true;
      }
    }

    //System.out.println("Obtained event with tag "+ev_.get_tag()+" at "+Sim_system.clock());
  }
}
