/*
 * GridMonitorAdmin.java
 *
 * Created on January 1, 2003, 11:32 PM
 *
 * @author : Siddharth Rai
 *
 * GridMonitor class oversees entire system. 
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
  int INDEX_DHT_NODES    = 6;
  int FEEDBACK_DHT_NODES = 6;
  int count_nodes_system;
  int count_indexed_values;

  ArrayList indexnodes;
  ArrayList feedbacknodes;
  ArrayList brokerlist;    
  ArrayList grouplist;  //list of groups
  ArrayList queue;
  ArrayList deferedqueue;    
  ArrayList pending_event_list;
  
  Group currentgroup;   //cuurrent group of nodes

  boolean isstable;
  boolean indexingcomplete;
  boolean tracingtraffic;

  boolean ready;
  
  double reqtime;
  double arrivaltime;

  int node_to_node_latency;
  
  int peer;
  
  int ping_count;
  
  ClockPulseGenerator clockpulsegenerator;

  /** Creates a new instance of GridMonitorAdmin */
  public GridMonitorAdmin(String name) throws Exception
  {
    super(name);

    indexnodes    = new ArrayList();
    feedbacknodes = new ArrayList();
    brokerlist    = new ArrayList();
    deferedqueue  = new ArrayList();        
    
    pending_event_list = new ArrayList();
    
    queue = new ArrayList();

    nodeid = this.get_id();

    peer = -1;
    ping_count = 0;
    
    ready = true;
    
    //clockpulsegenerator   = new ClockPulseGenerator(this.nodeid, 4);
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
    int avg_hops = 1; 
    int latency_per_hop = 1; // Latency in micro seconds
    node_to_node_latency = avg_hops * latency_per_hop * 1;
    
    //clockpulsegenerator=new ClockPulseGenerator(this.nodeid);#
    
    clockpulsegenerator = null;
    
    System.out.println("Administartor node is up now ! nodeid " + nodeid);
  }

  /*
   * Listens for incoming events and sends event to {@link processEvent} for
   * processing
   *
   */

  public void body() 
  {
    Sim_event ev;
    Sim_event new_ev;

    System.out.println("In admin body with peer " + peer);
    
    while (clockpulsegenerator != null && clockpulsegenerator.isRunning()) 
    {
      /*
      if (ready == true && peer != -1)
      {
        sendPing(peer);
      }
      */
      
      ev = new Sim_event();
      
      this.getNextEvent(ev);

      if (ready == true && pending_event_list.size() > 0)
      {
        while (pending_event_list.size() > 0)
        {
          new_ev = (Sim_event) pending_event_list.remove(0);
      
          processEvent(new_ev);
        }
        
        ready = false;
      }

      ev = null;
    }
    
    System.out.println("Admin finished");
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
        System.out.println("Request to get role from " + src + " at " + Sim_system.clock());
        
        queue.add(ev_);

        if (currentgroup == null)
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
          ;//break;
        }

        System.out.println("Source id:"+src+"Destination id:"+dest);
        
      case GridMonitorTags.JOIN_COMPLETE:
        if (queue.isEmpty() != true) 
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
              send(src, node_to_node_latency, GridMonitorTags.ROLE_INDEX_DHT_NODE, 
                  new GridMonitorIO(nodeid, src, null));
            } 
            else 
            {
              send(src, node_to_node_latency, GridMonitorTags.ROLE_INDEX_DHT_NODE, 
                  new GridMonitorIO(nodeid, src, (Object)indexnodes.get(0)));
            }   
            
            System.out.println("Node " + src + " is index DHT node, total index node " + indexnodes.size());
          }
          else 
          {
            if (feedbacknodes.size() < FEEDBACK_DHT_NODES) 
            {
              feedbacknodes.add(((GridMonitorIO)ev_.get_data()).getsrc());

              if (feedbacknodes.size() == 1) 
              {
                send(src,node_to_node_latency, GridMonitorTags.ROLE_FEEDBACK_ADMIN, 
                    new GridMonitorIO(nodeid, src, null));
              } 
              else 
              {
                send(src, node_to_node_latency, GridMonitorTags.ROLE_FEEDBACK_DHT_NODE, 
                    new GridMonitorIO(nodeid, src, (Object)feedbacknodes.get(0)));
              }
              
              System.out.println("Node " + src + " is feedback DHT node, total feedback node " + feedbacknodes.size());
            } 
            else 
            {
              send(src,node_to_node_latency, GridMonitorTags.ROLE_RESOURCE, 
                  new GridMonitorIO(nodeid, src, (Object)indexnodes.get(0)));
              
              System.out.println("Node " + src + " is resource node ");
            }
          }
        }
        else 
        {
          // TODO: initiate processing deferred queue.
          
          System.out.println("join completed..."+deferedqueue.size());
          
          i = deferedqueue.iterator();

          while(i.hasNext()) 
          {
            ev_ = (Sim_event)deferedqueue.remove(0);

            System.out.println("Processing " + ev_.get_tag());
            
            if (ev_.get_tag() != GridMonitorTags.ADD_BROKER) 
            {
              processOtherEvents(ev_);
            } 
            else 
            {
              deferedqueue.add(ev_);
            }

            // this.send(dest,10.0,GridMonitorTags.START_DHT_INDEXING,new GridMonitorIO(this.nodeid,dest,null));
          }

          while(deferedqueue.isEmpty()!=true) 
          {
            ev_ = (Sim_event)deferedqueue.remove(0);

            processOtherEvents(ev_);
          }

          isstable = true;
        }
        
        break;

      case GridMonitorTags.ADD_BROKER:
        processOtherEvents(ev_);
        break;

      default:
        //processOtherEvents(ev_);
        
        if (isstable != true) 
        {
          deferedqueue.add(ev_);
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
       case GridMonitorTags.PONG:
        System.out.println("Received PONG at " + clockpulsegenerator.getPulseCount());         
        break;
          
      case GridMonitorTags.ADD_BROKER:
        //System.out.println("Request at admin node to add Broker added with id " + 
        //    src + " from " + this.nodeid);
        
        if (isstable == true && indexingcomplete == true) 
        {
          System.out.println("Broker added with id " + src);
          
          brokerlist.add(data);

          this.send(src, node_to_node_latency, GridMonitorTags.BROKER_ADDED, 
              new GridMonitorIO(nodeid, src, (Object)indexnodes.get(0)));
        } 
        else 
        {
          this.send(src, node_to_node_latency, GridMonitorTags.RETRY, 
              new GridMonitorIO(nodeid, src, null));
          
          //System.out.println("Sent Broker RETRY to " + src);
        }

        break;

      case GridMonitorTags.GET_A_INDEX_NODE:
        //System.out.println("Request for an index node from " + src + " at " + dest);
        this.send(src, node_to_node_latency, GridMonitorTags.A_INDEX_NODE, 
            new GridMonitorIO(this.nodeid, src, (Object)indexnodes.get(0)));
        break;

      case GridMonitorTags.GET_A_FEEDBACK_NODE:
        //System.out.println("Request for an feedback node from " + src + " at " + dest);
        this.send(src, node_to_node_latency, GridMonitorTags.A_FEEDBACK_NODE, 
            new GridMonitorIO(this.nodeid, src, (Object)feedbacknodes.get(0)));
        break;

      case GridMonitorTags.COUNT:
        count_indexed_values += 1;
        //System.out.println("Indexed values " + count_indexed_values);
        
        if (this.count_indexed_values >= 5) 
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

            //System.out.println("Leader is " + src);

            this.send(src, node_to_node_latency, GridMonitorTags.GROUP, new GridMonitorIO(this.nodeid, src, currentgroup.getMember()));
          }

          src = (Integer)this.feedbacknodes.get(0);
          this.send(src, node_to_node_latency, GridMonitorTags.START, new GridMonitorIO(this.nodeid, src, null));

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
        ///this.sim_process(1.0);

        //if(pingrequest)
        System.out.println("Received PING " + ((GridMonitorIO)ev_.get_data()).getdata() + " at " + clockpulsegenerator.getPulseCount());
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

        HashCode.computeConsistentHash(val, hashkey);

        reqtime = Sim_system.clock();
        // this.send(src,node_to_node_latency,GridMonitorTags.FIND_SUCCESSOR,new GridMonitorIO(this.nodeid,src,hashkey));
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

      //System.out.println("Node " + this.nodeid + " received event " + ev_.get_tag());
      
      if(ev_.get_tag() == GridMonitorTags.CLOCK_PULSE && Sim_system.running())
      {            
        pulse = (ClockPulse)(((GridMonitorIO)ev_.get_data()).getdata());                           
        //serviceDemonTask(pulse.getPulseCount());               

        //System.out.println("Clock pulse:"+pulse.getPulseCount()+" received at "+this.nodeid);
        ready = true;
        finish = true;
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
      
        pending_event_list.add(ev_);
        finish = true;
      }
    }
    //System.out.println("Obtained event with tag "+ev_.get_tag()+" at "+Sim_system.clock());
  }
  
  public void sendPing(int dest)
  {     
    //System.out.println("Request to send PING to " + dest + " from " + nodeid);
    ping_count++;
    send(dest, 0, GridMonitorTags.PING, new GridMonitorIO(nodeid, dest, ping_count));   
  }
  
  public int getNodeId()
  {
    return this.nodeid;
  }
  
  public void setPeerNode(int node)
  {
    peer = node;
  }
  
  public void setClockPulseGenerator(ClockPulseGenerator clock)
  {
    clockpulsegenerator = clock;
  }
}