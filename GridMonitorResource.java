/*
 * GridMonitorResource.java
 *
 * Created on March 14, 2008, 1:47 AM
 *
 * Author : Siddharth Rai
 */
package gridmonitor;

import eduni.simjava.*;
import gridsim.*;
import java.security.*;
import java.util.*;
import java.io.*;

public class GridMonitorResource extends GridSimCore {

  int nodeid;
  int role;
  double currentload;
  byte hashkey[];
  byte indexkey[];
  String name;
  int node_to_node_latency;
  ClockPulseGenerator clockpulsegenerator;
  GridMonitorDHTStub dht;

  boolean joined;
  boolean ready;
  
  int submit_count;
  
  //list and hash table for group member current load
  ArrayList memberid;
  HashMap membertable;

  // Id of Admin node
  int gridmonitoradminid;
  int isid;
  int leader;

  /////////////////////////////////////////////////////////
  //used by feedback node to compute total system utilization
  int feedbacknodeid;//feedback node known to all nodes
  int utilizationfeedcount;
  int totalfeedback;

  int updatecount; //no of nodes allowed to update at a time
  int previous_membertablesize;
  int free;
  int underutilized;
  int overutilized;
  double utilization;

  int index_count;
   
  //Token token;
  /////////////////////////////////////////////////////////
  int totaljobsreceived;
  /////////////////////////////////////////////////////////
  //hash map is used as a ptr table to current feed back node id is use for key value
  HashMap feedbackptrtable;

  HashCode hashcode;
  
  //////////////////////////////////////////////////////////
  //nodes that actually host the resource used in key lookup
  ArrayList update;

  //////////////////////////////////////////////////////////  
  //used for mutual exclusion of feedback updation
  boolean isupdating;

  ////////////////////////////////////////////////////////// 
  boolean mutex;

  ////////////////////////////////////////////////////////
  //used for sending feedback load to feedback node timely bool is used for identifing SUCCESSOR message
  boolean issendingfeedback;
  double feedbackload;
  
  ArrayList pending_event_list;

  ///////////////////////////////////////////////////////
  /**
   * Characteristics of this resource
   */
  protected ResourceCharacteristics resource_;

  /**
   * a ResourceCalendar object
   */
  protected gridmonitor.ResourceCalendar resCalendar_;

  /**
   * A resource's scheduler. This object is reponsible in scheduling and and
   * executing submitted Gridlets.
   */
  protected gridmonitor.AllocPolicy policy_;

  /**
   * A scheduler type of this resource, such as FCFS, Round Robin, etc
   */
  protected int policytype_;

  private String os, arch;
  private double time_zone, cost;

  private double baud_rate;
  private long seed;
  private double peakLoad;
  private double offPeakLoad;
  private double holidayLoad;

  //////////////////////////////////////////////////////////////////////////
  private ArrayList deferredqueue;
  boolean isconnected;

  /////////////////////////////////////////////////////////////////////////
  //trace files 
  FileWriter dhtmessages;
  FileWriter arrivedmessages;
  RandomAccessFile localload, util, updaterequestcount;

  ////////////////////////////////////////////////////////////////////////
  //message counter and time to manage bandwidth delay
  int messagecount;
  int dhtmessagecount;
  int groupmessagecount;
  double arrivaltime;
  int bandwidth;

  boolean with_gridlet;
  boolean with_grupd;
  
  ////////////////////////////////////////////////////////////////////////
  int localjobrate;

  Sim_event last_event;
  
  /**
   * Creates a new instance of GridMonitorResource param name: name of the
   * entity as required by sim_entity param gridadminid_:id of administrator
   * node
   */
  public GridMonitorResource(String name, int gridmonitoradminid_, RandomAccessFile util_, ArrayList clocked_nodes, boolean with_gridlet_, boolean with_grupd_) throws Exception {
    super(name);
    Random rand = new Random();

    this.name = name;
    gridmonitoradminid = gridmonitoradminid_;
    nodeid = this.get_id();
    
    with_gridlet = with_gridlet_;
    with_grupd = with_grupd_;
    submit_count = 0;
    
    hashcode = new HashCode();
    
    clocked_nodes.add(nodeid);
    
    //System.out.println("With gridlet set to " + with_gridlet + " for resource " + this.nodeid);
    
    currentload = 0.0;
    hashkey = new byte[20];
    int avg_hops = 1; 
    int latency_per_hop = 1; // Latency in micro seconds
      
    node_to_node_latency = avg_hops * latency_per_hop * 1;
    ready = true;
    
    // Compute hashkey for the resource out of the node-id.
    //hashcode.compute(nodeid, hashkey);
    hashcode.computeConsistentHash(nodeid, hashkey);
      
    //System.out.println("Node hash key = " + hashcode.getString(hashkey) + " for node " + nodeid + " node name " + name);
    
    dht = new GridMonitorDHTStub(this);

    joined = false;
    deferredqueue = new ArrayList();
    feedbackptrtable = new HashMap();
    membertable = new HashMap();
    memberid = null;
    leader = -1;
    //utilization metrics initialized/////////////////////////////////
    utilizationfeedcount = 0;
    totalfeedback = 0;

    updatecount = 0;
    previous_membertablesize = 0;
    utilization = 0.0;
    free = 0;
    underutilized = 0;
    overutilized = 0;
    //token =new Token();

    index_count = 0;
    
    pending_event_list = new ArrayList();
    
    //////////////////////////////////////////////////////////////////
    isconnected = false;
    isupdating = false;
    issendingfeedback = false;
    mutex = false;
    initResource();
    initAllocationPolicy(clocked_nodes);
    
    //clockpulsegenerator = new ClockPulseGenerator(this.nodeid, 4);
    
    //dhtmessages = new FileWriter("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_dht_message_" + this.nodeid + ".dat");
    //util = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_load_" + this.nodeid + ".dat", "rw");
    //arrivedmessages = new FileWriter("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_arrived_message_rate_" + this.nodeid + ".dat");
    //util = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_load_" + this.nodeid + ".dat", "rw");
    //updaterequestcount = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_updaterequestcount_" + this.nodeid + ".dat", "rw");

    arrivaltime = 0.0;
    messagecount = 0;
    dhtmessagecount = 0;
    groupmessagecount = 0;
    bandwidth = 20;
    localload = new RandomAccessFile("./inputdata/das2_fs0.dat", "r");
    localjobrate = 1;
    //hit=0;
    //miss=0;
    //util=util_;
    //System.out.println("Respurce id is:"+nodeid);

  }

  private void initResource() {
    int i, mcount;

    arch = "Sun Ultra";      // system architecture
    os = "Solaris";          // operating system

    time_zone = 9.0;         // time zone this resource located

    cost = 3.0;              // the cost of using this resource

    baud_rate = 10000.0;           // communication speed

    seed = 11L * 13 * 17 * 19 * 23 + 1;

    peakLoad = 0.0;     // the resource load during peak hour
    offPeakLoad = 0.0;     // the resource load during off-peak hr
    holidayLoad = 0.0;     // the resource load during holiday

    // incorporates weekends so the grid resource is on 7 days a week
    LinkedList weekends = new LinkedList();
    MachineList mList = new MachineList();

    PEList peList1;

    for (mcount = 0; mcount < 4; mcount++) {
      peList1 = new PEList();

      for (i = 0; i < 4; i++) {
        peList1.add(new PE(i, 120));  // need to store PE id and MIPS Rating
      }

      //peList1.add( new PE(1, 377) );
      //peList1.add( new PE(2, 377) );
      //peList1.add( new PE(3, 377) );
      mList.add(new Machine(mcount, peList1));
    }

    try {
      resource_ = new ResourceCharacteristics(arch, os, mList, ResourceCharacteristics.TIME_SHARED, time_zone, cost);
      //System.out.println("Resource Characteristics created..."+resource_.getMIPSRating());

      weekends.add(new Integer(Calendar.SATURDAY));
      weekends.add(new Integer(Calendar.SUNDAY));

      // incorporates holidays. However, no holidays are set in this example
      LinkedList holidays = new LinkedList();

      resCalendar_ = new ResourceCalendar(resource_.getResourceTimeZone(), peakLoad, offPeakLoad, holidayLoad, weekends, holidays, seed);
    } catch (Exception e) {
      System.out.println("Exception in GMResource at " + e.getStackTrace()[0].getLineNumber() + " " + e.getMessage());
    }
  }

  //////////////////////////////////////////////////////////////////////////
  /**
   *
   * @param args the command line arguments
   */
  public void body() {
    int src;
    int delay;
    boolean waiting;
    
    IndexEntry indexentry;
    Sim_event ev;

    Iterator i;

    ev = new Sim_event();

    indexkey = new byte[20];

    waiting = false;
    delay   = 0;
    
    //FeedbackIndexEntry feedbackindexentry;
    try {
      //String start;
      //String end;
      //byte startkey[]=new byte[20];
      //byte endkey[]=new byte[20];
      //System.out.println(resource_);
      //hashcode.compute(resource_.getMIPSRatingOfOnePE(),this.indexkey);
      //System.out.println("Seeking Role to Administrator node...at "+Sim_system.clock());
      getRole(ev);

      // System.out.println("Obtained Role at "+Sim_system.clock());
      // Both DHT-INDEX and DHT-FEEDBACK nodes join the DHT in the same way.
      if (role != GridMonitorTags.ROLE_RESOURCE) {
        System.out.println("Node " + this.nodeid + " joining dht at " + Sim_system.clock());
        joinDHT(ev);

        this.send(gridmonitoradminid, node_to_node_latency, GridMonitorTags.JOIN_COMPLETE, new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null, false));

        //if(role==GridMonitorTags.ROLE_FEEDBACK_DHT_NODE)
        {
          //   this.sim_pause(10.0);
          //   this.send(gridmonitoradminid,node_to_node_latency,GridMonitorTags.GET_A_INDEX_NODE,new GridMonitorIO(this.nodeid,gridmonitoradminid,null));
        }

        System.out.println("Node "+this.nodeid+" Joined the DHT....with successor "+dht.successor+"and predecessor "+dht.predecessor);
      } else {
        //this.send(gridmonitoradminid,node_to_node_latency,GridMonitorTags.JOIN_COMPLETE,new GridMonitorIO(this.nodeid,this.gridmonitoradminid,null));

        //this.sim_pause(10.0);
        //this.send(gridmonitoradminid,node_to_node_latency,GridMonitorTags.GET_A_INDEX_NODE,new GridMonitorIO(this.nodeid,gridmonitoradminid,null));
        //src=(Integer)(((GridMonitorIO)ev.get_data()).getdata());
        //this.send(src,node_to_node_latency,GridMonitorTags.FIND_SUCCESSOR,new GridMonitorIO(this.nodeid,src,(Object)this.indexkey));
        // this.getNextEvent(ev);
        //src=(Integer)(((GridMonitorIO)ev.get_data()).getdata());
        //this.send(src,node_to_node_latency,GridMonitorTags.INDEX,new GridMonitorIO(this.nodeid,src,(Object)this.indexkey));
        System.out.println("Other resources indexed the system");
        
        this.send(gridmonitoradminid, node_to_node_latency, GridMonitorTags.JOIN_COMPLETE, new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null, false));
      }

      System.out.println("Entity " + this.name + " joined at " + Sim_system.clock());

      this.send(this.gridmonitoradminid, node_to_node_latency, GridMonitorTags.GET_A_FEEDBACK_NODE, new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null, false));
      this.send(this.gridmonitoradminid, node_to_node_latency, GridMonitorTags.GET_A_INDEX_NODE, new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null, false));

      // Start of indexing
      this.policy_.index();
      
      //this.getNextEvent(ev);

      ev = null;

      while (Sim_system.running() && clockpulsegenerator.isRunning()) {
        ev = new Sim_event();

        /*
        if (pending_event_list.size() > 0)
        {
         if (((Sim_event)(pending_event_list.get(0))).get_tag() == GridMonitorTags.INDEX_FEEDBACK)
         {
           clockpulsegenerator.setWaitTime(nodeid, 3);
         }
         else
         {
           clockpulsegenerator.setWaitTime(nodeid, 1);
         }        
         
         //System.out.println("Waittime set");
        }
        */
        
        /*
        if (waiting == true)
        { 
          System.out.println("[NODE DEALY] Going to wait for " + delay + " events waiting " + pending_event_list.size());
          
          while (delay > 0)
          {  
            Sim_event wait_ev = new Sim_event();
            
            this.getNextEvent(wait_ev);
            
            if (ready == true)
            {
              ready = false;
            
              delay = delay - 1;
              
              wait_ev = null;
            }           
          }
          System.out.println("[NODE DELAY] Wait over events waiting " + pending_event_list.size());
        }
        */
        
        this.getNextEvent(ev);
                       
        /* 
        if (ev.get_tag() == GridMonitorTags.INDEX_FEEDBACK)
         {
           sim_pause(50);
         }
         else
         {
           //if (ev.get_tag() == GridMonitorTags.KEY_LOOKUP)
           //{
           //   System.out.println("Key lookup at " + Sim_system.clock());
           //}
           sim_pause(10);
         }
       */
         /*
        if (ev.get_src() != -1) 
        {
          processEvent(ev);
        }
        */
        
        if (ready == true)
        {
          //System.out.println("Going to poll for events with pending events " + pending_event_list.size());
          int processed = 0;
          while (processed < 8 && pending_event_list.size() > 0)
          //while (processed < 500 && pending_event_list.size() > 0)
          //while (pending_event_list.size() > 0)
          {
            if (pending_event_list.size() > 0)
            {
              ev = (Sim_event)pending_event_list.remove(0);
      
              System.out.println("[PROCESSING] Processing msg " + ev.get_tag() + " at node " + this.nodeid);
              waiting = false;
            
              processEvent(ev);
              
              //if (ready == true)
              {
                if (ev.get_tag() == GridMonitorTags.INDEX)
                {
                  clockpulsegenerator.setWaitTime(nodeid, 15);
                  
                  System.out.println("[RESOURCE WAIT]Setting wait time 15 at node " + this.nodeid);
                  
                  //break;
                }
                else
                {
                  System.out.println("[RESOURCE PROCESS]Setting wait time 3 at node " + this.nodeid);
                  clockpulsegenerator.setWaitTime(nodeid, 0);
                }
              }  
              //System.out.println("Processed " + ev.get_tag() + " at node " + this.nodeid);
            
              ready = false;
            
              ev = null;
              
              processed = processed + 1;
            }
          }
        }
        
        /*
        if (pending_event_list.size() > 0 && ready == false)
        {
          System.out.println("Resource " + this.nodeid + " not ready to process the event " + pending_event_list.size());
        }
        else if (pending_event_list.size() > 0)
        {
          System.out.println("Unknown blockage");
        }
        */
      }
      
      //System.out.println("Total Jobs received :"+this.totaljobsreceived);

      //dhtmessages.close();
      //arrivedmessages.close();
      //util.close();
      //updaterequestcount.close();

      //System.out.println("Finished at " + Sim_system.clock() + " index size at " + this.nodeid + " is :" + this.dht.index.size() + "\n-------------------");
      i = dht.index.iterator();

      if (this.nodeid < 13) 
      {
        while (i.hasNext()) 
        {
          indexentry = (IndexEntry) i.next();
          indexentry.getHashkey(indexkey);
          //System.out.println(hashcode.getString(indexkey) + " " + indexentry.getId());
        }
      } 
      else 
      {
        while (i.hasNext()) 
        {
          indexentry = (IndexEntry) i.next();
          indexentry.getHashkey(indexkey);
          //System.out.println(hashcode.getString(indexkey) + " " + indexentry.getId());
        }
      }

      //System.out.println("Number of index req received at node " + this.nodeid + " = " + this.dht.indexreq + " " + index_count);
      //System.out.println("----------------------");
    } 
    catch (Exception e) 
    {
      System.out.println("Exception in GMResource at " + e.getStackTrace()[0].getLineNumber() + " " + e.getMessage());
    }
  }

  //////////////////////////////////////////////////////////////////////////
  /*
   *
   *
   *
   */
  void initAllocationPolicy(ArrayList clocked_nodes) {
    try {
      policy_ = new gridmonitor.TimeShared(name, "TimeShared", gridmonitoradminid, nodeid, clocked_nodes, with_gridlet, with_grupd);
      policytype_ = ResourceCharacteristics.TIME_SHARED;
      policy_.init(resource_, resCalendar_, new Sim_port("output"));
    } catch (Exception e) {
      System.out.println("exception during allocation policy initialization");
    }
  }

  //////////////////////////////////////////////////////////////////////////
  /*
   *Function obtains role from administrator
   *
   */
  private void getRole(Sim_event ev) {
    Object data;

    try {
      send(gridmonitoradminid, node_to_node_latency, GridMonitorTags.ROLE_GET, new GridMonitorIO(nodeid, gridmonitoradminid, null, false));

      this.getNextEvent(ev);

      while (ev.get_tag() == GridMonitorTags.CLOCK_PULSE)
      {
        this.getNextEvent(ev);
      }
      
      System.out.println("Received event " + ev.get_tag() + " for role at " + this.nodeid);
      
      role = ev.get_tag();

      isid = 2;

      /*data=((GridMonitorIO)ev.get_data()).getdata();            
        if(data!=null)
        isid=(Integer)data;
        else
        isid=this.nodeid;
        System.out.println(this.nodeid+" received "+isid+" as isid");
       */
    } catch (Exception e) {
      System.out.println("Exception " + e.getMessage());
    }
  }

  //////////////////////////////////////////////////////////////////////////
  /*
   *Function sends request to join DHT
   *arg ev : sim event object returned by admin node
   */
  private void joinDHT(Sim_event ev) {
    //this.sim_pause(10.0*this.nodeid);
    int dest, predecessor, successor;
    Sim_event rply = new Sim_event();

    if (((GridMonitorIO) ev.get_data()).getdata() != null) {
      dest = (Integer) (((GridMonitorIO) ev.get_data()).getdata());
      send(dest, node_to_node_latency, GridMonitorTags.FIND_SUCCESSOR, new GridMonitorIO(nodeid, dest, (Object) hashkey, false));
      this.getNextEvent(rply);
      
      while (rply.get_tag() == GridMonitorTags.CLOCK_PULSE)
      {
        this.getNextEvent(rply);
      }
      
      //ready = true;
      
      System.out.println("Received event " + rply.get_tag() + " for join DHT");
      
      //System.out.println(dest +" "+nodeid);
      dest = (Integer) (((GridMonitorIO) rply.get_data()).getdata());
      send(dest, node_to_node_latency, GridMonitorTags.DHT_JOIN, new GridMonitorIO(nodeid, dest, (Object) hashkey, false));
      
      this.getNextEvent(rply);
      
      while (rply.get_tag() == GridMonitorTags.CLOCK_PULSE)
      {
        this.getNextEvent(rply);
      }
      
      if (rply.get_tag() == GridMonitorTags.JOINED) {
        predecessor = ((JoiningMessage) ((GridMonitorIO) rply.get_data()).getdata()).getPredecessor();
        successor = ((JoiningMessage) ((GridMonitorIO) rply.get_data()).getdata()).getSuccessor();

        dht.predecessor = predecessor;
        dht.successor = successor;
        dht.successorlist = (ArrayList) (((JoiningMessage) ((GridMonitorIO) rply.get_data()).getdata()).getSuccessorList().clone());
        
        //System.out.println("[DHT JOIN] node-id = " + this.nodeid + " " + dht.successorlist.size() + " " + dht.predecessor + " " + dht.successor);
        
        int waiting_for_reply = 1;
        
        this.send(predecessor, node_to_node_latency, GridMonitorTags.SET_SUCCESSOR, new GridMonitorIO(this.nodeid, predecessor, null, false));
        
        while (waiting_for_reply > 0)
        {
          this.getNextEvent(rply);
          
          while (rply.get_tag() == GridMonitorTags.CLOCK_PULSE)
          {
            this.getNextEvent(rply);
          }
          
          if (rply.get_tag() == GridMonitorTags.SUCCESSOR_SET)
          {
            waiting_for_reply -= 1;
          }
          else
          {
            processEvent(rply);
            //System.out.println("[SET SUCCESSOR] Received reply " + rply.get_tag());
          }
        }
         
        this.send(successor, node_to_node_latency, GridMonitorTags.SET_PREDECESSOR, new GridMonitorIO(this.nodeid, successor, null, false));
      
        waiting_for_reply = 1;
        
        while (waiting_for_reply > 0)
        {
          this.getNextEvent(rply);
          
          while (rply.get_tag() == GridMonitorTags.CLOCK_PULSE)
          {
            this.getNextEvent(rply);
          }
          
          if (rply.get_tag() == GridMonitorTags.PREDECESSOR_SET)
          {
            waiting_for_reply -= 1;
          }
          else
          {
            processEvent(rply);
          }
        }
        // TODO: Send to admin that join is over
       
      } else {
        System.out.println("Reply out of order...." + rply.get_tag());
    
      }

      //System.out.println("Joined DHT with id "+dest);
    } else {
      dht.initFirstNode(this.hashkey);
      //System.out.println("This is the first node....");
      ///initialize DHT for single node
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  /*
   *function processes all incoming events
   */
  private void processEvent(Sim_event ev) {
    int src;
    int i;
    IndexEntry indexentry;
    byte hashkey[] = new byte[20];
    
    //System.out.println("[SIM EVENT PROCESSED] Processing event " + ev.get_tag() +
    //    " at " + this.nodeid + " from " + ev.get_src());    

    try {
      switch (ev.get_tag()) {
        case GridMonitorTags.START:
          joined = true;
          break;
          
        case GridMonitorTags.VERIFY:
          //System.out.println("Request for verification arrived...");
          this.serviceVerify(ev);
          this.processDeferredQueue();
          break;
          
        case GridMonitorTags.INDEX_FEEDBACK:
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          FeedbackRequest feedbackrequest = (FeedbackRequest) (((GridMonitorIO) ev.get_data()).getdata());
          int resource = feedbackrequest.getResource();
          int successorid = feedbackrequest.getSuccessorId();
          int temp;//temporary location for previous successor node for this node in feedback index
          IndexEntry feedbackindexentry = (IndexEntry) feedbackrequest.getFeedbackIndexEntry();
          //System.out.println("message for indexing a feedback arrived from "+src);
          if (feedbackptrtable.containsKey(resource) == true) {
            //System.out.println("$$$$$$$$$$$$$$$$$entry already present$$$$$$$$$$$$$$$$$$$$$$4");
            temp = (Integer) feedbackptrtable.remove(resource);
            this.send(temp, node_to_node_latency, GridMonitorTags.REMOVE_FEEDBACK, new GridMonitorIO(this.nodeid, temp, resource, false));
          }

          feedbackptrtable.put(resource, successorid);
          this.totalfeedback++;
          this.send(successorid, node_to_node_latency, GridMonitorTags.INDEX, new GridMonitorIO(this.nodeid, successorid, feedbackindexentry, false));
          //System.out.println("Indexed feedback from resource "+feedbackindexentry.getLoad()+"of "+resource+" from "+src);
          if (this.totalfeedback == 10) {
            this.totalfeedback = 0;
            this.updateFeedback();
          }
          //index to map
          //send index request
          //System.out.println("feedback indexing finished");
          break;

        case GridMonitorTags.SET_INDEXNODE:

          src = ((GridMonitorIO) ev.get_data()).getsrc();
          if (policy_.indexnode != -1) {
            indexentry = (IndexEntry) ((GridMonitorIO) ev.get_data()).getdata();
            indexentry.getHashkey(hashkey);
            //removing previous value
            this.send(policy_.indexnode, node_to_node_latency, GridMonitorTags.REMOVE, new GridMonitorIO(this.nodeid, policy_.indexnode, indexentry, false));
          }
          
          //System.out.println("Req to set index node to " + src + " arrived at " + this.nodeid + " for val " + hashcode.getString(hashkey));
          
          policy_.indexnode = src;

          break;

        case GridMonitorTags.A_FEEDBACK_NODE:
          feedbacknodeid = (Integer) ((GridMonitorIO) ev.get_data()).getdata();
          //System.out.println("Node " + this.nodeid + " received " + feedbacknodeid + " as feedback node");
          break;

        case GridMonitorTags.UTILIZATION_FEED:
          utilizationfeedcount++;
          if (((GridMonitorIO) ev.get_data()).getdata() != null) {
            UtilizationFeed feed = (UtilizationFeed) (((GridMonitorIO) ev.get_data()).getdata());
            utilization += feed.getUtilization();
            totalfeedback += feed.getSize();
            free += feed.getFree();
            underutilized += feed.getUnderUtilized();
            overutilized += feed.getOverUtilized();
          }
          
          /*
             if(utilizationfeedcount==6)
             {
             if(utilization!=0)
             {
             utilization=utilization/totalfeedback;                    
             System.out.println("System utilization is "+utilization+" when feed back collected "+totalfeedback); 
             System.out.println("Free are"+free+"\nUnderutilized are "+underutilized+"\nOverutilized "+overutilized);
             utilizationAction();
             }
             utilizationfeedcount=0;
             utilization=0.0;
             totalfeedback=0;
             free=0;
             underutilized=0;
             overutilized=0;
             }
           */
          break;
        case GridSimTags.GRIDLET_SUBMIT:
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          Gridlet gridlet = (Gridlet) (((GridMonitorIO) ev.get_data()).getdata());
          boolean update_index = ((GridMonitorIO)ev.get_data()).doUpdate();
          
          //System.out.println(gridlet.getGridletLength()+"Grid let submitted at  "+this.nodeid+" at time "+Sim_system.clock()+" when load is "+policy_.getTotalLoad().getLast()+"\n--------------------------------");
          boolean ack;
          
          if (src == this.nodeid) {
            ack = false;
          } else {
            ack = true;
          }

          policy_.gridletSubmit(gridlet, src, ack, update_index, false);

          //util.writeBytes(Double.toString(Sim_system.clock())+" "+Double.toString(policy_.getTotalLoad().getLast())+"\n");
          break;

        case GridMonitorTags.GROUP:
          memberid = (ArrayList) (((GridMonitorIO) ev.get_data()).getdata());
          i = 0;

          while (i < memberid.size()) {
            src = (Integer) memberid.get(i);
            //membertable.put(src,-1.0);
            //System.out.println(" "+memberentry.getNodeId());
            this.send(src, node_to_node_latency, GridMonitorTags.SET_LEADER, new GridMonitorIO(this.nodeid, src, null, false));
            i++;
          }

          break;

        case GridMonitorTags.SET_LEADER:
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          leader = src;
          //System.out.println("Node "+this.nodeid+" seted leader to "+leader);
          break;

        case GridMonitorTags.SET_LOAD:
          double load = (Double) ((GridMonitorIO) ev.get_data()).getdata();
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          membertable.put(src, load);

          //System.out.println("member size "+member.size()+" load "+load);
          break;

        case GridMonitorTags.SEND_FEEDBACK:
          this.checkCurrentLoad();
          break;

        default:
          serviceDHT(ev);
          //System.out.println("Going to service DHT " + ev.get_tag() + " from " + ev.get_src() + " at " + this.nodeid);
          break;
      }
    } catch (Exception e) {
    }
  }

  private void serviceDHT(Sim_event ev) {
    int id, src, dest, i, indexnode, resource, batchindexcount;
    double load;
    byte hashkey[], start[], end[], previoushashkey[];
    ArrayList successorlist;
    ArrayList batchindex;
    RangeQuery query, newquery;
    IndexEntry indexentry;
    //FeedbackIndexEntry feedbackindexentry;

    ArrayList destinationid;
    Iterator itr;
    
    try 
    {
      //System.out.println("Source id"+ev.get_src());
      //dhtmessages.write(Double.toString(Sim_system.clock())+"\n");
      
      switch (ev.get_tag()) {
        case GridMonitorTags.FIND_SUCCESSOR:
          //System.out.println("Request to find successor from " + ((GridMonitorIO) (ev.get_data())).getsrc() + " at " + this.nodeid);
          //System.out.println("Request receive time...."+Sim_system.clock()+" for "+ev.get_src());
          hashkey = (byte[]) ((GridMonitorIO) (ev.get_data())).getdata();

          id = dht.get_successor(hashkey);

          if (id == nodeid) {
            src = ((GridMonitorIO) (ev.get_data())).getsrc();
            this.send(src, node_to_node_latency, GridMonitorTags.SUCCESSOR, new GridMonitorIO(nodeid, src, nodeid, false));
            //System.out.println("Found successor node for " + src + " at " + this.nodeid);
          } else {
            //System.out.println("hash key "+hashcode.getString(hashkey)+"not found on "+this.nodeid);
            this.send(id, node_to_node_latency, GridMonitorTags.FIND_SUCCESSOR, ev.get_data());
          }
          break;

        case GridMonitorTags.DHT_JOIN:
          successorlist = new ArrayList();
          start = new byte[20];
          end = new byte[20];
          JoiningMessage rply;
          
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          dest = ((GridMonitorIO) ev.get_data()).getdest();
          hashkey = (byte[]) ((GridMonitorIO) (ev.get_data())).getdata();

          //System.out.println("Received request for DHT join from " + src + " at " + dest);
          
          if (dht.successorlist.size() == 2) {
            //System.out.println("predeecessor for initial node "+hashcode.getString(hashkey)+" at dht node "+hashcode.getString(this.hashkey)+" comparision is"+hashcode.compare(hashkey,this.hashkey));
            if (hashcode.compare(hashkey, this.hashkey) < 0) {
              for (i = 0; i < 20; start[i] = 0x00, i++);

              successorlist.add(new SuccessorList(start, hashkey));
              ((SuccessorList) (dht.successorlist.get(1))).getStart(start);
              ((SuccessorList) (dht.successorlist.get(1))).getEnd(end);
              successorlist.add(new SuccessorList(start, end));
              dht.successorlist.clear();
              start = hashkey.clone();
              hashcode.incrememt(start);
              
              System.out.println("[DHT JOIN] " + " joined DTH with " + hashcode.getString(start) + " " + hashcode.getString(hashkey) + " " + hashcode.getString(end));
              
              dht.successorlist.add(new SuccessorList(start, this.hashkey));
              rply = new JoiningMessage(successorlist, dht.predecessor, this.nodeid);
              this.send(src, node_to_node_latency, GridMonitorTags.JOINED, new GridMonitorIO(this.nodeid, src, (Object) rply, false));
            } else {
              ((SuccessorList) dht.successorlist.get(1)).getStart(start);
              end = hashkey.clone();
              successorlist.add(new SuccessorList(start, end));
              dht.successorlist.remove(1);
              start = hashkey.clone();
              hashcode.incrememt(start);
              for (i = 0; i < 20; end[i] = (byte) 0xff, i++);
              dht.successorlist.add(new SuccessorList(start, end));

              System.out.println("[DHT JOIN] " + " joined DTH with " + hashcode.getString(start) + " " + hashcode.getString(hashkey) + " " + hashcode.getString(end));
              
              rply = new JoiningMessage(successorlist, dht.predecessor, this.nodeid);
              this.send(src, node_to_node_latency, GridMonitorTags.JOINED, new GridMonitorIO(this.nodeid, src, (Object) rply, false));
            }
          } else {
            System.out.println("predecessor for non initial node " + hashcode.getString(hashkey) + 
                "at dht node " + hashcode.getString(this.hashkey));
            ((SuccessorList) dht.successorlist.get(0)).getStart(start);
            end = hashkey.clone();
            successorlist.add(new SuccessorList(start, end));
            dht.successorlist.clear();
            start = hashkey.clone();
            hashcode.incrememt(start);
            dht.successorlist.add(new SuccessorList(start, this.hashkey));
            System.out.println("[DHT JOIN] " + " joined DTH with " + hashcode.getString(start) + " " + hashcode.getString(hashkey) + " " + hashcode.getString(end));
            rply = new JoiningMessage(successorlist, dht.predecessor, this.nodeid);
            this.send(src, node_to_node_latency, GridMonitorTags.JOINED, new GridMonitorIO(this.nodeid, src, (Object) rply, false));
          }
        //if(hashcode.compare())

        case GridMonitorTags.SET_PREDECESSOR:
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          dht.predecessor = src;
          System.out.println("[DHT JOIN] Predecessor for " + this.nodeid + " set to " + src);
          
          this.send(src, node_to_node_latency, GridMonitorTags.PREDECESSOR_SET, new GridMonitorIO(this.nodeid, src, null, false));
          break;

        case GridMonitorTags.SET_SUCCESSOR:
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          dht.successor = src;
          System.out.println("[DHT JOIN] Successor for " + this.nodeid + " set to " + src);
          
          this.send(src, node_to_node_latency, GridMonitorTags.SUCCESSOR_SET, new GridMonitorIO(this.nodeid, src, null, false));
          break;
        /*
             case GridMonitorTags.A_INDEX_NODE:
             src=(Integer)(((GridMonitorIO)ev.get_data()).getdata());
             this.send(src,node_to_node_latency,GridMonitorTags.FIND_SUCCESSOR,new GridMonitorIO(this.nodeid,src,(Object)this.indexkey));
             break;
             case GridMonitorTags.SUCCESSOR:
             src=(Integer)(((GridMonitorIO)ev.get_data()).getdata());
             this.send(src,node_to_node_latency,GridMonitorTags.INDEX,new GridMonitorIO(this.nodeid,src,(Object)this.indexkey));
             break;
           **/

        case GridMonitorTags.INDEX: //if(isupdating!=true)
        {
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          Object data = ((GridMonitorIO) ev.get_data()).getdata();
          //entry.getHashkey(indexkey);

          //System.out.println("received index request....from " + src + " on " + 
          //    this.nodeid + " at " + Sim_system.clock() + " " + ((IndexEntry)data).getLoad());
          
          index_count++;
          
          dht.addToIndex(data);
          
          //System.out.println("indexed key is " + hashcode.getString(indexkey) + " by node " + src);
          
          this.send(src, node_to_node_latency, GridMonitorTags.INDEX_UPDATED, new GridMonitorIO(this.nodeid, src, null, false));
          this.send(src, node_to_node_latency, GridMonitorTags.SET_INDEXNODE, new GridMonitorIO(this.nodeid, src, data, false));

          this.send(this.gridmonitoradminid, node_to_node_latency, GridMonitorTags.COUNT, 
              new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null, false));
        }
        break;

        case GridMonitorTags.BATCH_INDEX:
          hashkey = new byte[20];
          previoushashkey = new byte[20];
          batchindexcount = 0;
          //System.out.println("Received batch index at "+Sim_system.clock());
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          batchindex = (ArrayList) ((GridMonitorIO) ev.get_data()).getdata();

          //System.out.println("received key "+hashcode.getString(hashkey)+" for batch indexing ");
          itr = batchindex.iterator();
          while (itr.hasNext()) {
            indexentry = (IndexEntry) itr.next();
            indexentry.getHashkey(hashkey);
            resource = indexentry.getId();
            load = indexentry.getLoad();
            if (dht.containsid(hashkey) == -1) {
              this.send(dht.successor, node_to_node_latency, GridMonitorTags.BATCH_INDEX, 
                  new GridMonitorIO(src, dht.successor, batchindex, false));
              break;
            } 
            else 
            {
              dht.addToIndex(new IndexEntry(load, hashkey, resource));
              batchindexcount++;
              //System.out.println("request to set index sent to :"+resource+" at "+Sim_system.clock()+" new index val "+hashcode.getString(hashkey));
              this.send(resource, node_to_node_latency, GridMonitorTags.SET_INDEXNODE, 
                  new GridMonitorIO(this.nodeid, resource, new IndexEntry(load, hashkey, resource), false));
              itr.remove();
            }
          }

          if (batchindex.isEmpty()) {
            this.send(src, node_to_node_latency, GridMonitorTags.BATCH_INDEXED, new GridMonitorIO(this.nodeid, src, null, false));
          }

          this.messagecount += (batchindexcount - 1);
          //System.out.println("batchindex of size " + batchindexcount + " arrived.");
          break;

        case GridMonitorTags.REMOVE:
          hashkey = new byte[20];
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          indexentry = (IndexEntry) (((GridMonitorIO) ev.get_data()).getdata());

          if (dht.removeFromIndex(indexentry) != -1) {
            this.send(src, node_to_node_latency, GridMonitorTags.REMOVED, new GridMonitorIO(this.nodeid, src, null, false));
          }

          indexentry.getHashkey(hashkey);
          //System.out.println("[DHT STUB] Removed " + hashcode.getString(hashkey) + " from node " + this.nodeid);
          break;

        case GridMonitorTags.REMOVE_FEEDBACK:
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          resource = (Integer) (((GridMonitorIO) ev.get_data()).getdata());
          itr = dht.index.iterator();

          while (itr.hasNext()) {
            indexentry = (IndexEntry) itr.next();
            if (indexentry.getId() == resource) {
              itr.remove();
              break;
            }
          }
          break;

        case GridMonitorTags.KEY_LOOKUP:
          hashkey = new byte[20];

          src   = ((GridMonitorIO) ev.get_data()).getsrc();//messgae source
          dest  = ((GridMonitorIO) ev.get_data()).getdest();//message destination
          query = (RangeQuery) ((GridMonitorIO) ev.get_data()).getdata();//query

          //System.out.println("Range query received at " + this.nodeid + " for " + hashcode.getString(query.getStart()) + " " + 
          //    hashcode.getString(query.getEnd()) + " at " + clockpulsegenerator.getPulseCount());
          
          id = dht.get_successor(query.getStart());

          //System.out.println("key looked up...by :"+src+" at time "+Sim_system.clock());
          if (id == this.nodeid) 
          {
            //System.out.println("node found "+this.nodeid+" "+dht.containsid(query.getEnd())+" "+dht.containsid(query.getStart()));
            if (dht.containsid(query.getEnd()) == dht.containsid(query.getStart())) 
            {
              //entire range is contained by current node
              //System.out.println("Final node with full range found");
              
              destinationid = (ArrayList) (dht.getIndex(query).clone());
              //System.out.println("Final node found.. source node id is "+src);
              this.send(src, node_to_node_latency, GridMonitorTags.KEY_RESOURCE, 
                  new GridMonitorIO(this.nodeid, src, destinationid, false));
            } 
            else 
            {
              //only subquery is contained at this node
              destinationid = (ArrayList) (dht.getIndex(query).clone());
              //System.out.println("subquery node found.."+Sim_system.clock()+" at node "+this.nodeid+" index size is"+destinationid.size());
              this.send(src, node_to_node_latency, GridMonitorTags.MORE_RESOURCE, 
                  new GridMonitorIO(this.nodeid, src, destinationid, false));

              //send query to next node
              ((SuccessorList) (dht.successorlist.get(0))).getEnd(hashkey);
              hashcode.incrememt(hashkey);
              //System.out.println("New Range query " + hashcode.getString(hashkey) + 
              //    " " + hashcode.getString(query.getEnd()));
              newquery = new RangeQuery(hashkey, query.getEnd());

              this.send(dht.successor, node_to_node_latency, GridMonitorTags.KEY_LOOKUP, 
                  new GridMonitorIO(src, dht.successor, (Object) newquery, false));
            }            
          }          
          else 
          {
            this.send(id, node_to_node_latency, GridMonitorTags.KEY_LOOKUP, new GridMonitorIO(src, id, (Object) query, false));
            //System.out.println("Range query " + hashcode.getString(hashkey) + 
            //      " " + hashcode.getString(query.getEnd()) + " not found at node " + this.nodeid); 
            //System.out.println("Not found in node "+this.nodeid);
            //System.out.println("Range query to successor " + id + " from " + this.nodeid);
          }
          
          break;

        case GridMonitorTags.UPDATE_FEEDBACK:
          //System.out.println("update req by "+ev.get_src());
          if (isupdating == false && this.issendingfeedback == false) {
            hashkey = new byte[20];

            src = ((GridMonitorIO) ev.get_data()).getsrc();//messgae source
            dest = ((GridMonitorIO) ev.get_data()).getdest();//message destination
            query = (RangeQuery) ((GridMonitorIO) ev.get_data()).getdata();//query

            id = dht.get_successor(query.getStart());

            if (id == this.nodeid) 
            {
              //System.out.println("node for subquery...");
              update = (ArrayList) (dht.getUpdate(query).clone());

              //System.out.println("Update size is "+update.size()+" obtained at "+Sim_system.clock()+" for update by node "+src);
              if (update.size() > 0) {
                isupdating = true;
                this.send(this.gridmonitoradminid, node_to_node_latency, GridMonitorTags.GET_A_INDEX_NODE, new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null, false));
              }

              //System.out.println("node found "+this.nodeid+" "+dht.containsid(query.getEnd())+" "+dht.containsid(query.getStart()));
              if (dht.containsid(query.getEnd()) != dht.containsid(query.getStart())) {
                //System.out.println("finishing node...");
                //only subquery is contained at this node                                                                                  
                ((SuccessorList) (dht.successorlist.get(0))).getEnd(hashkey);
                hashcode.incrememt(hashkey);
                newquery = new RangeQuery(hashkey, query.getEnd());
                this.send(dht.successor, node_to_node_latency, GridMonitorTags.UPDATE_FEEDBACK, new GridMonitorIO(src, dht.successor, (Object) newquery, false));
              }
            } else {
              this.send(id, node_to_node_latency, GridMonitorTags.UPDATE_FEEDBACK, new GridMonitorIO(src, id, (Object) query, false));
              //System.out.println("Not found in node "+this.nodeid);
            }

            hashkey = null;
          }

          break;

        case GridMonitorTags.A_INDEX_NODE:
          hashkey = new byte[20];

          src = (Integer) ((GridMonitorIO) ev.get_data()).getdata();
          indexentry = (IndexEntry) update.get(0);
          indexentry.getHashkey(hashkey);

          System.out.println("received index node " + ev.get_tag() + " " + src);
          this.send(src, node_to_node_latency, GridMonitorTags.FIND_SUCCESSOR, new GridMonitorIO(this.nodeid, src, hashkey, false));

          hashkey = null;
          indexentry = null;
          break;

        case GridMonitorTags.SUCCESSOR:
          if (this.issendingfeedback == true) {
            src = (Integer) ((GridMonitorIO) ev.get_data()).getsrc();
            sendFeedback(src);
          } else {
            if (isupdating == true) {
              src = (Integer) ((GridMonitorIO) ev.get_data()).getsrc();
              //System.out.println("Updated index of size "+update.size()+"sent to "+src+" at "+Sim_system.clock());
              this.send(src, node_to_node_latency, GridMonitorTags.BATCH_INDEX, new GridMonitorIO(this.nodeid, src, update.clone(), false));
              //isupdating=false;
            }
          }
          break;

        case GridMonitorTags.BATCH_INDEXED:
          isupdating = false;

        case GridMonitorTags.PRINT:
          String start_key;
          String end_key;

          byte startkey[] = new byte[20];
          byte endkey[] = new byte[20];

          for (i = 0; i < dht.successorlist.size(); i++) {
            ((SuccessorList) (dht.successorlist.get(i))).getStart(startkey);
            ((SuccessorList) (dht.successorlist.get(i))).getEnd(endkey);

            start_key = hashcode.getString(startkey);
            end_key = hashcode.getString(endkey);
            // System.out.println("Node having id "+this.nodeid+" and hash key "+hashcode.getString(this.hashkey)+" has successor range "+start_key+" to "+end_key);
          }
          break;

        case GridMonitorTags.GET_INDEX_SIZE:
          //System.out.println("Node with id" + this.nodeid + "has index size of" + this.dht.index.size());
          break;

        case GridMonitorTags.GET_LOAD:
          src = ((GridMonitorIO) ev.get_data()).getsrc();
          //System.out.println("Received req for load enquery...from"+src);
          this.send(src, node_to_node_latency, GridMonitorTags.LOAD, new GridMonitorIO(this.nodeid, src, (Object) (policy_.getTotalLoad()), false));
          break;
      }
    } catch (Exception e) {
    }
  }

  /*
   *This procedure handles connection negotiation
   *@param ev_  simjava event object
   *@returns nothing
   */
  private void serviceVerify(Sim_event ev_) {
    Sim_event ev;
    byte[] hashkey = new byte[20];
    byte[] hashkeyreceived;
    double currentload;
    RangeQuery receivedquery;
    int src, dest;

    src = ((GridMonitorIO) ev_.get_data()).getsrc();
    dest = ((GridMonitorIO) ev_.get_data()).getdest();

    //System.out.println("verification request arrived...");
    try {
      //process verification

      currentload = policy_.getTotalLoad().getLast();
      hashcode.computeConsistentHash(currentload, hashkey);
      //System.out.println("Current load "+currentload);
      //hashkeyreceived=(byte[])(((GridMonitorIO)ev_.get_data()).getdata());
      receivedquery = (RangeQuery) (((GridMonitorIO) ev_.get_data()).getdata());

      //if(hashcode.compare(hashkey,hashkeyreceived)==0)
      if (receivedquery.contains(hashkey) == true) {
        isconnected = true;
        this.send(src, node_to_node_latency, GridMonitorTags.VERIFY_ACCEPT, new GridMonitorIO(this.nodeid, src, currentload, false));

        while (isconnected == true) {
          ev = new Sim_event();
          this.getNextEvent(ev);

          if (ev.get_tag() == GridSimTags.GRIDLET_SUBMIT) {
            src = ((GridMonitorIO) ev.get_data()).getsrc();
            Gridlet gridlet = (Gridlet) (((GridMonitorIO) ev.get_data()).getdata());
            //System.out.println(gridlet.getGridletLength()+"Grid let submitted at  "+this.nodeid+" at time "+Sim_system.clock()+" when load is "+policy_.getTotalLoad().getLast()+"\n--------------------------------");
            policy_.gridletSubmit(gridlet, src, true, false, false);
            isconnected = false;
            ev = null;
            //System.out.println("Differed queue size "+deferredqueue.size());
          } else {
            //src=((GridMonitorIO)ev.get_data()).getsrc();
            //dest=((GridMonitorIO)ev.get_data()).getdest();
            //System.out.println("added to deferred queue...event having tag "+ev.get_tag()+" from node "+src+" at "+this.nodeid);
            //deferredqueue.add(ev);
            pending_event_list.add(ev);
          }
        }

      } else {
        //System.out.println("verify rejected as load is "+currentload+" query "+hashcode.getString(receivedquery.getStart())+" "+hashcode.getString(receivedquery.getEnd()));
        this.send(src, node_to_node_latency, GridMonitorTags.VERIFY_REJECT, new GridMonitorIO(this.nodeid, src, currentload, false));
      }
      //System.out.println("verify response send...to "+src);
    } catch (Exception e) {
      System.out.println("Exception in grid monitor resource-service verify" + e.getMessage());
    }
  }

  /*
   *This function processes events received while processing connection negotiation
   *
   */
  private void processDeferredQueue() {
    Sim_event ev;
    while (deferredqueue.isEmpty() != true) {
      ev = (Sim_event) deferredqueue.remove(0);
      System.out.println("deferred event with tag "+ev.get_tag()+" from "+((GridMonitorIO)ev.get_data()).getsrc());
      switch (ev.get_tag()) {
        case GridMonitorTags.VERIFY:
          this.serviceVerify(ev);
          break;
        default:
          this.processEvent(ev);
          break;
      }
      ev = null;
    }

    //System.out.println(" finished processing deferred events ...");
  }

  private void getNextEvent(Sim_event ev_) {
    boolean finish = false;
    ClockPulse pulse;
    int count = 0;
    
    //System.out.println("Received next event at GMresource " + ev_.get_tag());
    
//System.out.println("Paused");
    //this.sim_pause(5.0);

    //System.out.println("Paused over");
    try 
    {
      //this.sim_pause(.001);
      if (messagecount == bandwidth && this.role != GridMonitorTags.ROLE_FEEDBACK_ADMIN && 
          this.role != GridMonitorTags.ROLE_FEEDBACK_DHT_NODE) 
      {
        //this.sim_pause(10.0);
        //arrivedmessages.write("----pause at "+messagecount+"message arrival at "+arrivaltime+"----\n");
        //messagecount=0;
      }

      while (!finish && clockpulsegenerator.isRunning()) 
      {
        count++;
        
        this.sim_get_next(ev_);
       
        if (ev_.get_tag() == GridMonitorTags.INDEX)
        {
          System.out.println("RESOURCE INDEX");
        }
        
        //this.sim_pause(1.0);
        if (ev_.get_tag() == GridMonitorTags.A_INDEX_NODE || ev_.get_tag() == GridMonitorTags.A_FEEDBACK_NODE)
        {
          System.out.println("Message arrived with tag " + ev_.get_tag() + " from " + ev_.get_src());
        }
        
        //if (ev_.get_tag() == GridMonitorTags.CLOCK_PULSE && clockpulsegenerator.isRunning() == true) {
        if (ev_.get_tag() == GridMonitorTags.CLOCK_PULSE) 
        {
          pulse = (ClockPulse) (((GridMonitorIO) ev_.get_data()).getdata());
         
          serviceDemonTask(clockpulsegenerator.getPulseCount());
        
          ready = true;
          
          //System.out.println("Clock pulse: " +pulse.getPulseCount() + " received at " + this.nodeid + " count " + count + " events " + pending_event_list.size());
          
          finish = true; 
        } 
        else 
        {
          //if (ready == false)
          {
            pending_event_list.add(ev_);
          }
          //else
          {
          //arrivedmessages.write("message arrived "+ev_.get_tag()+"\n");
          //System.out.println("[SIM EVENT] Message received " + ev_.get_tag() + 
          //    " seq no " + messagecount+ " at " + this.nodeid + " from " + 
          //    ev_.get_src() + " pending event " + pending_event_list.size());
          
          //ready = false;
          if (arrivaltime == Sim_system.clock()) {
            messagecount++;
            if (ev_.get_tag() > 200 && ev_.get_tag() < 300) {
              dhtmessagecount++;
            }
          } else {
            //arrivedmessages.write(messagecount + " " + arrivaltime + "\n");
            //dhtmessages.write(dhtmessagecount + " " + arrivaltime + "\n");
            messagecount = 1;
            if (ev_.get_tag() > 200 && ev_.get_tag() < 300) {
              dhtmessagecount = 1;
            } else {
              dhtmessagecount = 0;
            }

            arrivaltime = Sim_system.clock();
           
          }
          }
                    
          finish = true;
        }
      }
      
      if (ev_ != null)
      {
        if (ev_.get_data() != null)
        {
          //arrivedmessages.write(ev_.get_tag()+" arrived at "+Sim_system.clock()+" from "+((GridMonitorIO)ev_.get_data()).getsrc()+"\n");
        }
      }
    } catch (Exception e) {
      System.out.println("Exception in GMResource at " + e.getStackTrace()[0].getLineNumber() + " " + e.getMessage());
    }
  }

  private void serviceDemonTask(long pulse_) {
    Random rand = new Random();

   try {
      if ((pulse_ % 10) == 0) {
        switch (this.role) {
          case GridMonitorTags.ROLE_FEEDBACK_ADMIN:
            if (joined == true) //updateFeedback();
            {
              this.totalfeedback = 0;
            }
            //getUtilization();
            break;
        }
      }

      if (pulse_ % 2 == 0) {
        if (this.memberid != null) {
          //findCandidate();
        }
        //checkCurrentLoad();
      }

      //if(pulse_%4==0)
      {
        if (leader != -1) {
          //util.writeBytes(Double.toString(Sim_system.clock()) + " " + Double.toString(policy_.getTotalLoad().getLast()) + "\n");
          //checkIn();                 
        }
      }

      if (pulse_ % localjobrate == 0) {
        localjob();
        //System.out.println("[RESOURCE] Local job submitted on node "+this.nodeid+" at "+Sim_system.clock());
      }

    } 
    catch (Exception e) 
    {
      System.out.println("Exception in demon task ");
    }
  }

  /*
   *function to schedule local job
   */
  private void localjob() throws Exception {   
    double joblength = -1.0;
    String line;
    Gridlet g1;

    for (int i = 0 ; i < 4; i++)
    {
      do 
      {
        try 
        {
          line = localload.readLine();
          joblength = Double.parseDouble(line);
        }
        catch (EOFException e) 
        {
          localload.seek(0);
          joblength = -1.0;
        }
      }while (joblength == -1.0);
    
    //if (with_gridlet == true)
    {
      g1 = new Gridlet(0, joblength * 120.0, 300, 300);
      
      if (submit_count < 8)
      {
        this.policy_.gridletSubmit(g1, this.nodeid, false, true, true);
        submit_count += 1;
      }
      else
      {
        this.policy_.gridletSubmit(g1, this.nodeid, false, true, true);
        submit_count = 0;
      }
      
      //System.out.println("[RESOURCE] Local job submitted on " + this.nodeid + " at " + Sim_system.clock());
    }
    }
  }

  /*
   *function send current load to group leader
   *
   */
  private void checkIn() {
    double load = this.policy_.getTotalLoad().getLast();
    IndexEntry indexentry;
    //byte hashcode[] = new byte[20];

    if (load != currentload) {
      currentload = load;
      this.send(this.leader, node_to_node_latency, GridMonitorTags.SET_LOAD, new GridMonitorIO(this.nodeid, this.leader, currentload, false));
      //System.out.println("node "+this.nodeid+" checkedin value "+load +" at "+Sim_system.clock());

      if (policy_.indexnode != -1) {
        hashcode.computeConsistentHash(currentload, hashkey);
        indexentry = new IndexEntry(currentload, hashkey, this.nodeid);
        this.send(policy_.indexnode, node_to_node_latency, GridMonitorTags.REMOVE, new GridMonitorIO(this.nodeid, policy_.indexnode, indexentry, false));
        //System.out.println("value of node "+this.nodeid+" has been removed ");
        policy_.indexnode = -1;
      }
    }

    // if(load!=currentload)
    {
      //     currentload=load;
    }
  }

  /*
   *function find candidate node for current feedback
   */
  private void findCandidate() {
    Iterator i;
    int id, count = 5, total = 0, updated = 0;

    //System.out.println("__________________________________");     
    ArrayList free = new ArrayList();
    ArrayList underutilized = new ArrayList();
    ArrayList overutilized = new ArrayList();
    MemberEntry memberentry;
    DoubleComparator comp = new DoubleComparator();
    double load;
    //System.out.println(membertable.size()+" at "+Sim_system.clock());

    try {
      if (membertable.size() > 0) {
        if (membertable.size() > updatecount) {
          updatecount += 2;
        } else {
          if (membertable.size() < updatecount && updatecount >= 4) {
            updatecount -= 2;
          } else {
            if (membertable.size() <= 2) {
              updatecount = 2;
            }
          }
        }

        previous_membertablesize = membertable.size();
        i = memberid.iterator();

        while (i.hasNext()) {
          id = (Integer) i.next();

          if (membertable.containsKey(id)) {
            load = (Double) membertable.get(id);
            if (load >= 0.0 && load <= .67) {
              free.add(new MemberEntry(id, load));
            } else {
              if (load > 0.67 && load <= .91) {
                underutilized.add(new MemberEntry(id, load));
              } else {
                overutilized.add(new MemberEntry(id, load));
              }
            }
          }
          //i.remove();
        }

        //memberid.clear();
        Collections.sort(free, comp);
        Collections.sort(underutilized, comp);
        Collections.sort(overutilized, comp);
        total = free.size() + underutilized.size();
        //+overutilized.size();

        i = free.iterator();
        count = Math.round((free.size() * updatecount) / total);
        updated += (count < free.size()) ? count : free.size();

        while (i.hasNext() && count > 0) {
          count--;
          memberentry = (MemberEntry) i.next();
          id = memberentry.getNodeId();
          load = memberentry.getLoad();
          //System.out.println(memberentry.getNodeId()+" "+memberentry.getLoad());
          this.send(id, node_to_node_latency, GridMonitorTags.SEND_FEEDBACK, new GridMonitorIO(this.nodeid, memberentry.getNodeId(), null, false));
          membertable.remove(id);
        }

        i = underutilized.iterator();
        count = Math.round((underutilized.size() * updatecount) / total);
        updated += (count < underutilized.size()) ? count : underutilized.size();
        while (i.hasNext() && count > 0) {
          count--;
          memberentry = (MemberEntry) i.next();
          id = memberentry.getNodeId();
          load = memberentry.getLoad();
          //System.out.println(memberentry.getNodeId()+" "+memberentry.getLoad());
          this.send(memberentry.getNodeId(), node_to_node_latency, GridMonitorTags.SEND_FEEDBACK, new GridMonitorIO(this.nodeid, memberentry.getNodeId(), null, false));
          membertable.remove(id);

        }

        i = overutilized.iterator();
        count = Math.round((overutilized.size() * updatecount) / total);
        updated += (count < overutilized.size()) ? count : overutilized.size();

        while (i.hasNext() && count > 0) {
          count--;
          memberentry = (MemberEntry) i.next();
          id = memberentry.getNodeId();
          load = memberentry.getLoad();
          //System.out.println(memberentry.getNodeId()+" "+memberentry.getLoad());
          this.send(memberentry.getNodeId(), node_to_node_latency, GridMonitorTags.SEND_FEEDBACK, new GridMonitorIO(this.nodeid, memberentry.getNodeId(), null, false));
          membertable.remove(id);
        }

        //updaterequestcount.writeBytes(Sim_system.clock() + " " + (free.size() + underutilized.size() + overutilized.size()) + " " + updated + " " + updatecount + "\n");
        free.clear();
        underutilized.clear();
        overutilized.clear();

        //membertable.clear();
      }

    } catch (Exception e) {
      System.out.println("Exception in GMResource at " + e.getStackTrace()[0].getLineNumber());
    }

    //System.out.println("__________________________________");
  }

  /*
   *function that calculates current utilization
   *
   *
   */
  private void getUtilization() {

    int size = 0;
    size = ((this.dht).index).size();
    if (size > 0) {
      //System.out.println("Utilization is "+util);         
      this.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.UTILIZATION_FEED, new GridMonitorIO(this.nodeid, this.feedbacknodeid, new UtilizationFeed(this.dht.index), false));
    } else {
      this.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.UTILIZATION_FEED, new GridMonitorIO(this.nodeid, this.feedbacknodeid, null, false));
    }
  }

  private void checkCurrentLoad() {
    byte[] hashkey = new byte[20];

    feedbackload = this.policy_.getTotalLoad().getLast();

    //System.out.println("********************current system load at "+this.nodeid+" *************************"+feedbackload);
    if (issendingfeedback == false && this.feedbacknodeid != 0 && this.isupdating == false) {
      //currentload=feedbackload;
      issendingfeedback = true;
      hashcode.computeConsistentHash(feedbackload, hashkey);
      //System.out.println("CHECK CURRENT LOAD ISID"+this.isid+" at node "+this.nodeid);
      this.send(this.isid, node_to_node_latency, GridMonitorTags.FIND_SUCCESSOR, new GridMonitorIO(this.nodeid, this.isid, hashkey, false));
    }

    /*if(feedbackload>currentload)
      {
      currentload=feedbackload;
      }
     */
  }

  private void sendFeedback(int src) {
    byte hashkey[] = new byte[20];

    double load = this.policy_.getTotalLoad().getLast();

    if (load == this.feedbackload) {
      hashcode.computeConsistentHash(this.feedbackload, hashkey);
      //FeedbackRequest feedbackrequest=new FeedbackRequest(this.nodeid,src,new IndexEntry(this.feedbackload,hashkey,this.nodeid));
      //this.send(this.feedbacknodeid,node_to_node_latency,GridMonitorTags.INDEX_FEEDBACK,new GridMonitorIO(this.nodeid,this.feedbacknodeid,feedbackrequest));
      this.send(src, node_to_node_latency, GridMonitorTags.INDEX, 
          new GridMonitorIO(this.nodeid, src, new IndexEntry(this.feedbackload, hashkey, this.nodeid), false));
      //System.out.println("Sending feedback");
    }

    this.issendingfeedback = false;
    //System.out.println("***********send Feedback************** "+this.nodeid+ " current load is "+feedbackload);
  }

  private void updateFeedback() {
    byte rangestart[] = new byte[20];
    byte rangeend[] = new byte[20];
    RangeQuery updatequery;
    hashcode.computeConsistentHash(0.0, rangestart);
    hashcode.computeConsistentHash(0.99, rangeend);
    updatequery = new RangeQuery(rangestart, rangeend);
    this.send(this.nodeid, node_to_node_latency, GridMonitorTags.UPDATE_FEEDBACK, new GridMonitorIO(this.nodeid, this.nodeid, updatequery, false));
  }

  private void utilizationAction() {
    int i, action = -1;
    RangeQuery updatequery[] = new RangeQuery[3];
    byte rangestart[] = new byte[20];
    byte rangeend[] = new byte[20];

    for (i = 0; i < 3; i++) {
      switch (i) {
        case 0:
          hashcode.computeConsistentHash(0.0, rangestart);
          hashcode.computeConsistentHash(0.67, rangeend);
          updatequery[0] = new RangeQuery(rangestart, rangeend);
          break;
        case 1:
          hashcode.computeConsistentHash(0.67, rangestart);
          hashcode.computeConsistentHash(0.86, rangeend);
          updatequery[1] = new RangeQuery(rangestart, rangeend);
          break;
        case 2:
          hashcode.computeConsistentHash(0.86, rangestart);
          hashcode.computeConsistentHash(0.99, rangeend);
          updatequery[2] = new RangeQuery(rangestart, rangeend);
          break;
      }
    }

    //action free system
    if (utilization >= 0 && utilization < .67) {
      action = 0;
      this.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.UPDATE_FEEDBACK, new GridMonitorIO(this.nodeid, this.feedbacknodeid, updatequery[0], false));
    }

    //action for underutilized system
    if (utilization > .67 && utilization <= .86) {
      action = 1;
      if (underutilized > overutilized) {
        //update underutilized
        this.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.UPDATE_FEEDBACK, new GridMonitorIO(this.nodeid, this.feedbacknodeid, updatequery[1], false));
      } else {
        //update overutilized 
        this.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.UPDATE_FEEDBACK, new GridMonitorIO(this.nodeid, this.feedbacknodeid, updatequery[0], false));
      }
    }

    //action for overutilized system
    if (utilization > .86 && utilization <= .99) {
      action = 2;
      // update overutilized
      this.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.UPDATE_FEEDBACK, new GridMonitorIO(this.nodeid, this.feedbacknodeid, updatequery[2], false));
    }

    //System.out.println("Action " + action + " taken");
  }
    
  public int getNodeId()
  {
    return this.nodeid;
  }
  
  public void setClockPulseGenerator(ClockPulseGenerator clock_)
  {
    clockpulsegenerator = clock_;
  }
  
  public long getSubmitted()
  {
    return policy_.getSubmitted();
  }
  
   public long getDropped()
  {
    return policy_.getDropped();
  }
}