/*
 * GridMonitorBroker.java
 *
 * Created on January 1, 2003, 2:17 PM
 *
 * Implements a Grid broker object. 
 *
 * @author Siddharth Rai
 *
 */

package gridmonitor;

import eduni.simjava.*;
import gridsim.*;
import java.util.*;
import java.io.*;

public class GridMonitorBroker extends GridSimCore
{
  int gridmonitoradminid;
  int feedbacknodeid;
  int nodeid;

  //double jobqueue[]={3500000.0,2222020.0,550000.0,100000.0,35000.0,34560.0,12000.0,10000.0,7000.0,2500.0};
  ArrayList jobqueue;

  ClockPulseGenerator clockpulsegenerator;

  int    isid;
  double clock;
  double querytime;
  int    last_queryid;  
  byte   hashkey[];
  int    nodes;
  int    job_scheduled;
  int    reqsend,rplyreceived;
  double searchrange[];
  int    node_count;
  
  RangeQuery queryset[];
  RangeQuery query;
  boolean    query_in_progress;
  
  int    index_query_calls;
  int    query_count;  
  double total_query_time;
  int    currentqueryno;
  double beginat;
  
  boolean with_gridlet;
  boolean with_grupd;
  
  int node_to_node_latency;
  
  int cost;//cost of a dht query

  boolean ready;
  
  int update_group_size;
  
  List<TreeMap<Integer, Integer>> update_schedule_table;
  
  int next_update_table[];
  int update_histogram[];
  
  List <List<Double>> pending_jobs_per_node;
      
  long last_job_schedule_cycle;
  long last_job_update_cycle;
  
  RandomAccessFile inputfile;
  RandomAccessFile timestamp;
  RandomAccessFile resultcount;
  RandomAccessFile result;
  RandomAccessFile querycost;
  RandomAccessFile localload;
  
  FileWriter out;
  FileWriter query_time;
  FileWriter job_count;

  ArrayList pending_event_list;
  
  int schedulecount;

  HashCode hashcode;
  
  /** Creates a new instance of GridMonitorBroker */
  public GridMonitorBroker(String name_, int gridmonitoradminid_, int total_nodes, String inputfile_, ArrayList clocked_nodes, boolean with_gridlet_, boolean with_grupd_) throws Exception 
  {
    super(name_);

    int i;
    
    hashkey = new byte[20];

    gridmonitoradminid = gridmonitoradminid_;

    nodeid = this.get_id();

    with_gridlet = with_gridlet_;
    with_grupd = with_grupd_;
    
    node_count = total_nodes;
    
    /*
    if (with_gridlet == true)
    {
      System.out.println("Running with gridlet");
    }
    */
    hashcode = new HashCode();
    
    clocked_nodes.add(nodeid);
  
    update_group_size = 4;
    
    update_schedule_table = new ArrayList<TreeMap<Integer, Integer>>();
    
    for (i = 0; i < total_nodes; i++)
    {
      update_schedule_table.add(new TreeMap<Integer, Integer>());
    }
    
    next_update_table = new int[total_nodes];
    
    update_histogram = new int[total_nodes * 2 + 1];
    
    for (i = 0; i < total_nodes * 2 + 1; i++)
    {    
        update_histogram[i] = 0;
    }
    
    for (i = 0; i < total_nodes; i++)
    {    
        next_update_table[i] = 1 + i * (update_group_size * 2);
        
        //System.out.println("Next update table for group " + i + " initialized to " + next_update_table[i]);
        
        //next_update_table[i] = -1;
        //update_histogram[i] = 4;
    }
 
    localload = new RandomAccessFile("./inputdata/das2_fs0.dat", "r");
    
    pending_jobs_per_node = new ArrayList<List<Double>>();
    
    for (i = 0; i < total_nodes * 2 + 1; i++)
    {         
      pending_jobs_per_node.add(new ArrayList());
    }
  
    last_job_schedule_cycle = 0;
    last_job_update_cycle   = 0;
    
    //clockpulsegenerator = new ClockPulseGenerator(this.nodeid, 4);
    //clockpulsegenerator = clock_;
    
    //hashcode.compute(nodeid, hashkey);
    hashcode.computeConsistentHash(nodeid, hashkey);
    
    reqsend       = 0;
    rplyreceived  = 0;

    int avg_hops = 1; 
    int latency_per_hop = 1; // Latency in micro seconds
    
    node_to_node_latency = avg_hops * latency_per_hop * 1;
    
    out = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_schedule_trace_"+this.nodeid+".dat");
    
    //query_time = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_query_time_" + this.nodes + ".csv");
    //job_count = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_job_count_" + this.nodes + ".csv");
    
    System.out.println(inputfile_);
    
    inputfile   = new RandomAccessFile("./inputdata/lcg_arrival_abs.dat","r");
    timestamp   = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_timestamp.dat", "rw");
    resultcount = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_resultcount.dat", "rw");
    result      = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_result.dat", "rw");
    querycost   = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_querycost.dat", "rw");

    searchrange = new double[10];        

    searchrange[0] = 0.0;
    searchrange[1] = 0.25;
    searchrange[2] = 0.50;
    searchrange[3] = 0.75;
    searchrange[4] = 0.90;
    searchrange[5] = 1.00;
    
    last_queryid = 4;
    
    queryset = new RangeQuery[6];

    beginat = 0.0;
    clock   = 0.0;

    schedulecount = 0;

    jobqueue = new ArrayList();

    index_query_calls = 0;
    //joblength=new double[10];
    
    System.out.println("Broker started with id " + this.nodeid);
  }

  public void body() 
  {
    int queryid;
    int src;
    int dest;
    int queuejobcount = 0;

    Accumulator temp;

    Sim_event ev = new Sim_event();
    Random rand  = new Random();

    byte querystart[] = new byte[20];
    byte queryend[]   = new byte[20];

    long size = 0;
    long time = 0;

    double uptime     = 0.0;
    double downtime   = 0.0;
    double upduration = 0.0;
    double delay      = 0.0;

    String line;
    String parts[];

    //BufferedReader inputline=new BufferedReader(inputfile)
  
    ready = true;
    
    pending_event_list = new ArrayList();
        
    try
    {      
      do
      {
        this.send(gridmonitoradminid, node_to_node_latency, GridMonitorTags.ADD_BROKER, 
            new GridMonitorIO(this.nodeid, this.gridmonitoradminid, (Object)this.nodeid, false));
        
        //System.out.println("Waiting for Broker added ");
        
        this.getNextEvent(ev);
                          
        if (pending_event_list.size() > 0)
        {
          ev = (Sim_event)pending_event_list.remove(0);
          processEvent(ev);
        }
        
        //System.out.println("Event Tag "+ev.get_tag());
        //sim_pause(2.0);
      }while(ev.get_tag() != GridMonitorTags.BROKER_ADDED);

      System.out.println("Broker added");
      
      //System.out.println("Tag is"+ev.get_tag());
      isid = (Integer)(((GridMonitorIO)ev.get_data()).getdata());

      this.send(gridmonitoradminid, node_to_node_latency, GridMonitorTags.GET_A_FEEDBACK_NODE, 
          new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null, false));

      do
      {
        this.getNextEvent(ev);
        if (pending_event_list.size() > 0)
        {
          ev = (Sim_event)pending_event_list.remove(0);
          processEvent(ev);
        }
      }while(ev.get_tag() != GridMonitorTags.A_FEEDBACK_NODE);
      
      feedbacknodeid = (Integer)((GridMonitorIO)ev.get_data()).getdata();

      System.out.println("Broker is up now.....");   
      //System.out.println("Feed back node id is"+feedbackid);
      ev = null;

      //while(Sim_system.running())
      //this.sim_process(400.0);

      for(int i = 0; i < 6; i++)
      {
        switch (i)
        {
          case 0:
            hashcode.computeConsistentHash(searchrange[0],querystart);
            hashcode.computeConsistentHash(searchrange[1],queryend);            
            queryset[0] = new RangeQuery(querystart,queryend);    
            
            System.out.println("Query " + i + " range " + searchrange[0] + " " + searchrange[1] + " " + hashcode.getString(queryset[i].getStart()) + " " + hashcode.getString(queryset[i].getEnd()));
            break;

          case 1:
            hashcode.computeConsistentHash(searchrange[1],querystart);
            hashcode.computeConsistentHash(searchrange[2],queryend);            
            queryset[1] = new RangeQuery(querystart,queryend);
            
            System.out.println("Query " + i + " range " + searchrange[1] + " " + searchrange[2] + " " + hashcode.getString(queryset[i].getStart()) + " " + hashcode.getString(queryset[i].getEnd()));
            break;

          case 2:
            hashcode.computeConsistentHash(searchrange[2],querystart);
            hashcode.computeConsistentHash(searchrange[3],queryend);            
            queryset[2] = new RangeQuery(querystart,queryend);
            
            System.out.println("Query " + i + " range " + searchrange[2] + " " + searchrange[3] + " " + hashcode.getString(queryset[i].getStart()) + " " + hashcode.getString(queryset[i].getEnd()));
            break;

          case 3:
            hashcode.computeConsistentHash(searchrange[3],querystart);
            hashcode.computeConsistentHash(searchrange[4],queryend);            
            queryset[3] = new RangeQuery(querystart,queryend);
            
            System.out.println("Query " + i + " range " + searchrange[3] + " " + searchrange[4] + " " + hashcode.getString(queryset[i].getStart()) + " " + hashcode.getString(queryset[i].getEnd()));
            break;

          case 4:
            hashcode.computeConsistentHash(searchrange[4],querystart);
            hashcode.computeConsistentHash(searchrange[5],queryend);            
            queryset[4] = new RangeQuery(querystart,queryend);
            
            System.out.println("Query " + i + " range " + searchrange[4] + " " + searchrange[5] + " " + hashcode.getString(queryset[i].getStart()) + " " + hashcode.getString(queryset[i].getEnd()));
            break;

          case 5:
            hashcode.computeConsistentHash(searchrange[0],querystart);
            hashcode.computeConsistentHash(searchrange[5],queryend);                        
            queryset[5] = new RangeQuery(querystart, queryend);
            
            System.out.println("Query " + i + " range " + searchrange[0] + " " + searchrange[5] + " " + hashcode.getString(queryset[i].getStart()) + " " + hashcode.getString(queryset[i].getEnd()));
            break;
        }
        
        //System.out.println("Query " + i + " range " + searchrange[0] + " " + searchrange[5] + " " + hashcode.getString(queryset[i].getStart()) + " " + hashcode.getString(queryset[i].getEnd()));
      }

      uptime = Sim_system.clock();

      do
      {
        // Enqueue into job list
        double job_len;
        long duration;
        
        duration = ((long)Sim_system.clock()) - last_job_update_cycle;
        
        for (int node = 0; node < pending_jobs_per_node.size(); node++)
        {
          for (int job_index = 0; job_index < pending_jobs_per_node.get(node).size(); job_index++)
          {
            job_len = pending_jobs_per_node.get(node).get(job_index);
                     
            if (job_len <= duration)
            {
              pending_jobs_per_node.get(node).remove(job_index);
              //System.out.println("[BROKER] Job finished at node " + node + " job count " + pending_jobs_per_node.get(node).size() + " length " + job_len + " " + Sim_system.clock());
            }
            else
            {
              pending_jobs_per_node.get(node).set(job_index, job_len - duration);
              //System.out.println("[BROKER] At node " + node + ":" + job_index + " job count " + pending_jobs_per_node.get(node).size() + " length " + job_len + " duration " + duration + " clock " + Sim_system.clock());
            }
          }
        }
         
        last_job_update_cycle = (long)Sim_system.clock();
        
        //this.sim_process(1.0);
        //queryid = rand.nextInt(20);
        //queryid = 377;

        //currentqueryno=(upduration<=100)?0:(upduration<=200)?1:(upduration<=500)?2:3;
        //currentqueryno = rand.nextInt(6);
        //query=queryset[currentqueryno];
        //query = queryset[5];
        //System.out.println("query is "+hashcode.getString(query.getStart())+" "+hashcode.getString(query.getEnd()));

        delay = GridSimRandom.real(60,.9,.9,rand.nextDouble());

        //System.out.println("delay for broker with node id "+this.nodeid+"is "+delay);
        
        size = -1;
        queuejobcount = 512 - jobqueue.size();

        for (int i = 0 ; i < queuejobcount; i++)
        {
          do
          {
            try
            {
              line  = inputfile.readLine(); 
              parts = line.split(" ");

              time = Long.parseLong(parts[0]);
              size = Long.parseLong(parts[1]);  

              //System.out.println(time + " " + size * 120);
            }
            catch (EOFException e)
            {
              //System.out.println("EOF EXception");
              size = -1;    
              inputfile.seek(0);
            }
          }while (size <= 0);

          jobqueue.add(new GridJob(time, size));
        }

         //this.sim_pause(100.0);

        // Query logic:  Find as many resources as number of pending jobs. 
        
        cost = 0;
        
        //System.out.println("Range query " + last_queryid + " " + query.getStart() + " " + 
        //    query.getEnd() + " " + hashcode.getString(query.getStart()) + " " + hashcode.getString(query.getEnd()));
            
        ev = new Sim_event();
        this.getNextEvent(ev);
                
        
        if ((long)Sim_system.clock() - last_job_schedule_cycle >= 4000)
        {
               
          if (ev.get_tag() == GridMonitorTags.CLOCK_PULSE)
          {    
            // Update predicted job count 
            double joblength = -1.0;
          
            // Process current jobs
          
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
            
              // Enqueue into job list
              for (int node = 0; node < pending_jobs_per_node.size(); node++)
              {
                pending_jobs_per_node.get(node).add((120 * joblength));
              
                //System.out.println("[BROKER] Pending jobs = " + pending_jobs_per_node.get(node).size() + " at node " + node + " current job size " + 120 * joblength);
              }
            }
          }
          
          last_job_schedule_cycle = (long)Sim_system.clock();
        }
        
        if (ready == true)
        {
          //System.out.println("Going to poll for events with pending events " + pending_event_list.size());
          //queryid = rand.nextInt(5);
          query   = queryset[last_queryid];
        
          indexQuery();
                  
          if (pending_event_list.size() > 0)
          {
            ev = (Sim_event)pending_event_list.remove(0);
      
            //System.out.println("Processing " + ev.get_tag() + " at node " + this.nodeid);
                      
            processEvent(ev);
            
            //System.out.println("Processed " + ev.get_tag() + " at node " + this.nodeid);
            
            ready = false;
            
            ev = null;
          }
          
          ready = false;
        }
       
/*        
        if (ready == true)
        {        
  
        //System.out.println("no of req send by "+this.nodeid+" "+(++reqsend));
        //System.out.println("sending ping message...");
        //this.sim_pause(10.0);
        //this.send(this.gridmonitoradminid,node_to_node_latency,GridMonitorTags.PING,new GridMonitorIO(this.nodeid,this.gridmonitoradminid,null));
        //beginat=clockpulsegenerator.getPulseCount();
        
          ev = new Sim_event();

          this.getNextEvent(ev); 
        
          if (ev.get_src() != -1) 
          {
            System.out.println("Received event at broker with tag " + ev.get_tag());          
            processEvent(ev);
          }

          ev = null;

          downtime   = Sim_system.clock();
          upduration = downtime - uptime;
        }
  */      
        //System.out.println("Uptime " + uptime + " Downtime " + downtime + " Upduration " + upduration);
      }while(clockpulsegenerator.isRunning());       

      System.out.println("broker finished...");

      out.close();
      //query_time.close();
      //job_count.close();
      timestamp.close();
      resultcount.close();
      result.close();
    }
    catch(Exception e)
    {
      System.out.println("Execption at " + e.getStackTrace()[0].getLineNumber() + " " + e.getMessage());
    }
  }

  private void processEvent (Sim_event ev_) throws Exception
  {
    int src;
    int dest;
    int resource;
    int indexnode;        

    ArrayList resourceid = new ArrayList();
    ArrayList failedlist = new ArrayList();

    ArrayList   tempres;
    Accumulator temp;

    int id = 0;

    Iterator i;

    boolean finish = false;
    double length [] = {3500000.0,35000.0,2500.0,100000.0,7000.0,550000.0,34560.0,2222020.0,12000.0,10000.0};
    double currentlength;
    double delay = 0.0;
    double resulttime;
    double load;
    double indexedload;

    long file_size   = 3000;
    long output_size = 3000;

    Gridlet gridlet1 ;

    Random    rand  = new Random();
    Sim_event ev    = new Sim_event();

    byte hashkey[]      = new byte[20];
    byte temphashkey[]  = new byte[20];

    IndexEntry indexentry;

    //FeedbackIndexEntry feedbackindexentry;
    FeedbackRequest feedbackrequest;

    boolean verified = false;

    //System.out.println("Processing event from " + ev_.get_src() + " tag " + ev_.get_tag());
        
    switch(ev_.get_tag()) 
    {
      case GridMonitorTags.MORE_RESOURCE:
        //while(!finish)
        {
          if (ev_.get_tag() != GridMonitorTags.MORE_RESOURCE)
          {
            ;//System.out.println("Received out of band event " + ev_.get_tag());
          }
          else
          {
          cost++;    

          tempres = (ArrayList)((ArrayList)(((GridMonitorIO)ev_.get_data()).getdata())).clone();

          if (tempres.size() != 0)
          {
            for(int j = 0; j < tempres.size(); j++)
            {
              resourceid.add(tempres.get(j));
            }
          }

          //System.out.println("Found " + tempres.size() + " more resources");
          }
          
          /*
          this.getNextEvent(ev);

          ev_ = ev;

          if (ev_.get_tag() == GridMonitorTags.KEY_RESOURCE)
          {
            finish = true;                
            
            System.out.println("Query time at node " + ev_.get_src() + " = " + (clockpulsegenerator.getPulseCount() - querytime) + " " + clockpulsegenerator.getPulseCount());
            
            query_time.write(Sim_system.clock() + ";" + (clockpulsegenerator.getPulseCount() - querytime) + "\n");
            
            query_in_progress = false;
          }
          */
          //processEvent(ev);
        }
        break;
        
      case GridMonitorTags.KEY_RESOURCE: 
        src     = ((GridMonitorIO)ev_.get_data()).getsrc();
        dest    = ((GridMonitorIO)ev_.get_data()).getdest();
        tempres = (ArrayList)((ArrayList)(((GridMonitorIO)ev_.get_data()).getdata())).clone();

        if (tempres.size() != 0)
        {
          for (int j = 0; j < tempres.size(); j++)
          {
            resourceid.add(tempres.get(j));
          }
        }

        //querytime=Sim_system.clock();
        //query_time.write(Sim_system.clock() + ";" + (clockpulsegenerator.getPulseCount() - querytime) + "\n");
        
        total_query_time = total_query_time + (clockpulsegenerator.getPulseCount() - querytime);
        
        //System.out.println("Query time one hop at node " + src + " = " + (clockpulsegenerator.getPulseCount() - querytime) + 
        //    " " + clockpulsegenerator.getPulseCount());
        
        //System.out.println("Found " + tempres.size() + " resources");
        
        //store dht query cost
        querycost.writeBytes(cost + "\n");

        cost = 0;

        query_in_progress = false;
        
        if (resourceid.size() < 50)
        {
          //if (clockpulsegenerator.isRunning() == true)
          {
            //this.sim_pause(10.0);
            if (last_queryid == 0)
            {
              last_queryid = 5;
            }
            else
            {
              last_queryid = last_queryid - 1;
            }
            
            //query = queryset[last_queryid];
            query = queryset[5];
            //System.out.println("Range query " + last_queryid + " " + query.getStart() + " " + 
            //query.getEnd() + " " + hashcode.getString(query.getStart()) + " " + hashcode.getString(query.getEnd()));
            schedule(resourceid);
            this.indexQuery();
          }
        }
        else
        {
          schedule(resourceid);//schedule event ot newly arrived jobs
          //System.out.println("result set size "+resourceid.size());
          //ArrayList tempresourceid=(ArrayList)resourceid.clone();
          //Collections.shuffle(resourceid);
          tempres = null;
        }

        //System.out.println("Key held by node: " + resourceid);
        //System.out.println("--------------------------------------");
        //resulttime=Sim_system.clock();
        //IndexEntry entry;

        //System.out.println("Query is "+currentqueryno);
        resourceid = null;
        break;

      case GridMonitorTags.LOAD:
        temp = (gridmonitor.Accumulator)((GridMonitorIO)ev_.get_data()).getdata();
        //System.out.println("Load at resource having id:" + ((GridMonitorIO)ev_.get_data()).getsrc() + 
        //    " is " + temp.getLast());
        break;

      case GridMonitorTags.KEY_NOT_FOUND:
        break;

      case GridMonitorTags.PONG:
        //System.out.println("TAT for ping at " + this.nodeid + " response time " + 
        //    (clockpulsegenerator.getPulseCount() - beginat) + " send at " + beginat + " received at " + 
        //    clockpulsegenerator.getPulseCount());
        break;
        
      case GridSimTags.GRIDLET_SUBMIT_ACK:
          //System.out.println("[BROKER] Gridlet submit ack received");
          //System.out.println("[BROKER] Received event post gridlet submission " + ev_.get_tag());
          
          load      = (Double)((GridMonitorIO)ev_.get_data()).getdata();                                              
          resource  = (Integer)((GridMonitorIO)ev_.get_data()).getsrc();                        
          
          if (ev_.get_tag() == GridSimTags.GRIDLET_SUBMIT_ACK)
          {
            System.out.println("[BROKER] Submit complete with load " + load + " at src " + resource);
          }
          else
          {
            System.out.println("[BROKER] Random event received");
          }
          
          //hashcode.computeConsistentHash(load, temphashkey);

          //indexentry = new IndexEntry(load, temphashkey, resource);

          //sendFeedback(resource,indexentry,temphashkey);          
          
          //out.write(Double.toString(currentlength) + "    " + Integer.toString(src) + 
          //   "   " + Double.toString(indexedload) + "   " + load + "   " + i +
          //    Sim_system.clock() + "\n");
          
      default:
        ;//System.out.println("Recived unknown event " + ev_.get_tag() + " from " + ev_.get_src());
        
    }

    // System.out.println("Finished at "+Sim_system.clock());
  }


  private void getNextEvent (Sim_event ev_)
  {
    boolean finish = false;

    ClockPulse pulse;

    while (!finish && clockpulsegenerator.isRunning())
    {
      this.sim_get_next(ev_);

      //System.out.println("Broker received next event " + ev_.get_tag() + " from " + ev_.get_src());
      
      if (ev_.get_tag() == GridMonitorTags.CLOCK_PULSE)
      {            
        pulse = (ClockPulse)(((GridMonitorIO)ev_.get_data()).getdata());
        clock = (clock + 1) % 100000;
        
        //System.out.println("Broker received clock pulse: " + pulse.getPulseCount() + " received " + ev_.get_tag());
        
        finish = true;
        ready  = true;
      }
      else
      {     
        pending_event_list.add(ev_);
        finish = true;
        
        //System.out.println("Broker received a new event");
      }          
    }
  }

  /*
   * Schedules jobs currently in jobqueue to appropriate resource.
   *
   * Arguments:
   *  resourceid_ : Set of resources obtained in load range [0.0,.75]
   *
   */

  private void schedule (ArrayList resourceid_)
  {
    int i;
    int src;
    int resource;

    Gridlet gridlet1;

    long file_size    = 3000;
    long output_size  = 3000;

    long size = 0;
    long time = 0;

    double currentlength;
    double load;
    double indexedload;

    byte hashkey[]      = new byte[20]; // Hashkey obtailed from index
    byte temphashkey[]  = new byte[20]; // Hashkey for current load value

    Sim_event ev = new Sim_event();

    GridJob job;
    Iterator itr;
    IndexEntry indexentry;

    try
    {
      // Collections.shuffle(resourceid_);   
      // TODO: Divide resource into groups.             
      int next_nodeid;
      int group_id;
      int update_next_node[] = new int[node_count];
           
      //System.out.println("Nodes " + node_count + " scheduling");
       
      for (i = 0; i < node_count; i++)
      {
        update_schedule_table.get(i).clear();
        update_next_node[i] = -1;
      }
          
      for (i = 0; i < resourceid_.size(); i++)
      {       
        indexentry  = (IndexEntry)resourceid_.get(i);        
        next_nodeid = indexentry.getId();
        
        group_id = next_nodeid / (2 * update_group_size);
        
        update_schedule_table.get(group_id).put(next_nodeid, 1);        
      }
       
      for (i = 0; i < node_count; i++)
      {
        if (update_schedule_table.get(i).size() > 0)
        {
          update_next_node[i] = next_update_table[i] + 2;
        
          if (update_next_node[i] >= (i + 1) * (update_group_size * 2))
          {
            update_next_node[i] = 1 + (i * (update_group_size * 2));
          }
        
          //System.out.println("Updating node = " + update_next_node[i] + " for group " + i + " current update table value " + next_update_table[i]);
        }
      }
      
      /*
      for (i = 0; i < node_count; i++)
      {
        update_next_node[i] = -1;
        
        int e_size = update_schedule_table.get(i).size();
        
        for (int e_i = 0; e_i < e_size; e_i++)
        {
          int first_key = (int)(update_schedule_table.get(i).firstKey());
          
          if (first_key > next_update_table[i])
          {            
            update_next_node[i] = first_key;
            
            System.out.println("Updating node = " + first_key + " for group " + i);
            
            break;
          }
          
          update_schedule_table.get(i).remove(first_key);
        }
      }
      */
      
      IndexedValueComparator comp = new IndexedValueComparator();        

      //Collections.sort(resourceid_, comp);

      //System.out.println("__________________________________________________________\n" + 
      //    "Number of resources obtained:" + resourceid_.size() + " at " + Sim_system.clock());

      //out.write("Schedule number :"+schedulecount+"\n");
      schedulecount++;
      
      resultcount.writeBytes(Integer.toString(resourceid_.size())+"\n");
      
      //System.out.println("Number of resources obtained " + resourceid_.size() + " pending jobs " + jobqueue.size());

      // 200 is total resource count
      //for (i = 0; i < resourceid_.size() && i < 200 && jobqueue.size() > 0; i++)
      for (i = 0; i < resourceid_.size() && jobqueue.size() > 0; i++)
      {       
        job = (GridJob)jobqueue.get(0);
        
        currentlength = (double)(job.getSize()) * 120;
        
        //currentlength = (double)(10) * 120; 
        //if ((job.getTime()) <= Sim_system.clock())
        //if (with_gridlet == true)
        {
          jobqueue.remove(0);
          
          //if (with_gridlet == true) 
          {
            gridlet1    = new Gridlet(i, currentlength, file_size, output_size);
            indexentry  = (IndexEntry)resourceid_.get(i);

            src = indexentry.getId();

            //System.out.println("Next resource id " + src);
            
            indexedload = indexentry.getLoad();

            //System.out.println("Gridlet of length " + currentlength + " submitted to node " + src + " from node " + this.nodeid);
            
            //if (src == update_next_node[src / (update_group_size * 2)])
            //if (pending_jobs_per_node.get(src).size() < 32)
            {
              //if (update_histogram[src] == 0)
              if (with_grupd)
              {
                if (pending_jobs_per_node.get(src).size() > 32 && update_histogram[src] == 0)
                {
                //System.out.println("Sending update for group " + (src / (update_group_size * 2)) + " " + update_next_node[src / (update_group_size * 2)] + " size = " + update_schedule_table.get((src / (update_group_size * 2))).size());
              
                //System.out.println("Current node " + src + " next update node " + update_schedule_table.get((src / (update_group_size * 2))).firstKey() + " " + next_update_table[(src / (update_group_size * 2))]);
              
                  this.send(src, node_to_node_latency, 
                  GridSimTags.GRIDLET_SUBMIT, new GridMonitorIO(this.nodeid, src, gridlet1, true));
              
                  update_histogram[src] = 32;
                }            
                else
                {            
                  this.send(src, node_to_node_latency, 
                    GridSimTags.GRIDLET_SUBMIT, new GridMonitorIO(this.nodeid, src, gridlet1, false));
              
                  if (update_histogram[src] > 0)
                  {
                    update_histogram[src]  -= 1;
                  }
                }
              }
              else
              {
                this.send(src, node_to_node_latency, 
                  GridSimTags.GRIDLET_SUBMIT, new GridMonitorIO(this.nodeid, src, gridlet1, false));
              }
            }
            
            pending_jobs_per_node.get(src).add(currentlength);
            
            /*
            if (pending_jobs_per_node.get(src).size() >= 16)
            {
              System.out.println("[BROKER] Sending to overloaded machine ");
            }
            */
          }    
          
          job_scheduled = job_scheduled + 1;
          
        /*          
          this.getNextEvent(ev);
          
          try
          {
            while (ev.get_tag() != GridSimTags.GRIDLET_SUBMIT_ACK)
            {
              processEvent(ev);
              this.getNextEvent(ev);
            }
          }
          catch (Exception e)
          {
            System.out.println("Exception in process event from gridlet submission " + 
                e.toString() + " " + e.getMessage() + " " + e.getStackTrace()[0].getLineNumber());
          }
          
          System.out.println("Received event post gridlet submission " + ev.get_tag());
          
          load      = (Double)((GridMonitorIO)ev.get_data()).getdata();                                              
          resource  = (Integer)((GridMonitorIO)ev.get_data()).getsrc();                        
          
          if (ev.get_tag() == GridSimTags.GRIDLET_SUBMIT_ACK)
          {
            System.out.println("Submit complete with load " + load + " at src " + resource);
          }
          else
          {
            System.out.println("Random event received");
          }
          
          hashcode.computeConsistentHash(load, temphashkey);

          indexentry = new IndexEntry(load, temphashkey, resource);

          sendFeedback(resource,indexentry,temphashkey);          
          
          out.write(Double.toString(currentlength) + "    " + Integer.toString(src) + 
              "   " + Double.toString(indexedload) + "   " + load + "   " + i +
              Sim_system.clock() + "\n");
*/
          //System.out.println(Double.toString(currentlength) + "    " + 
          //    Integer.toString(src) + "   " + Double.toString(indexedload) + 
          //   "   " + load + "   " + Sim_system.clock()+"\n");
        }
      }

      //System.out.println("Jobs left in the queue " + jobqueue.size());
      
      for (i = 0; i < resourceid_.size(); i++)
      {
        indexentry = (IndexEntry)resourceid_.get(i);
        result.writeBytes(Double.toString(Sim_system.clock()) + " " + Double.toString(indexentry.getLoad()) + "\n");            
      }
      
      for (i = 0; i < node_count; i++)
      {
        if (update_next_node[i] != -1)
        {
          if (update_next_node[i] != ((i + 1) * update_group_size * 2) - 1)
          {
            next_update_table[i] = update_next_node[i];
          }
          else
          {
            next_update_table[i] = 1 + i * (update_group_size * 2);
          }
        }
      }
      
      //out.write("_______________________________________________________\n");
      timestamp.writeBytes(Double.toString(Sim_system.clock()) + "\n");
    }
    catch(IOException e)
    {
      System.out.println(e.toString() + " Exception " + e.getMessage());
    }
  }

  /*
   * Send feedback to feedback node
   *
   * Arguments:
   *
   *  resource    : Resourceid of feedback message(resource whose feedback is this)
   *  indexentry_ : Feedback indexentry
   *  hashkey     : Hashkey to be indexed
   */

  private void sendFeedback(int resource, IndexEntry indexentry_, byte []hashkey)
  {        
    int successor; 

    Sim_event ev = new Sim_event();

    FeedbackRequest feedbackrequest;

    try
    {
    super.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.FIND_SUCCESSOR, new GridMonitorIO(this.nodeid, this.feedbacknodeid, hashkey, false));

    this.getNextEvent(ev);

    if (ev.get_src() != -1)
    {
      //processEvent(ev);
    }
    
    successor = ((GridMonitorIO)ev.get_data()).getsrc();
    feedbackrequest = new FeedbackRequest(resource, successor, indexentry_);

    super.send(this.feedbacknodeid, node_to_node_latency, GridMonitorTags.INDEX_FEEDBACK, new GridMonitorIO(this.nodeid, this.feedbacknodeid, feedbackrequest, false));
    }
    catch (Exception e)
    {
      
    }
    
    //super.send(resource,node_to_node_latency,GridMonitorTags.REMOVE_INDEX_ENTRY,new GridMonitorIO(this.nodeid,resource,null));
    //super.send(this.feedbacknodeid,node_to_node_latency,GridMonitorTags.INDEX_FEEDBACK,new GridMonitorIO(this.nodeid,this.feedbacknodeid,feedbackrequest));
    //System.out.println("*********Feedback sent***********"+indexentry_.getLoad());
  }

  private void indexQuery()    
  {
    index_query_calls++;
    
    //if (query_in_progress == false && clockpulsegenerator.getPulseCount() % 8 == 0)
    //if (clockpulsegenerator.getPulseCount() % 8 == 0)
    if (query_in_progress == false)
    {  
      query_count++;
      
      query_in_progress = true;
    
      //System.out.println("Range query sent " + query_count + " " + index_query_calls + " " + 
      //    last_queryid + " at " + clockpulsegenerator.getPulseCount()+ " " + Sim_system.clock() + 
      //    " " + query.getStart() + " " + query.getEnd() + " " + hashcode.getString(query.getStart()) + " " + 
      //    hashcode.getString(query.getEnd()));
           
      this.send(this.isid, node_to_node_latency, GridMonitorTags.KEY_LOOKUP, new GridMonitorIO(this.nodeid, this.isid, (Object)query, false));
      querytime = clockpulsegenerator.getPulseCount();
    }
  }
  
  public int getNodeId()
  {
    return this.nodeid;
  }
  
  public void setClockPulseGenerator(ClockPulseGenerator clock_)
  {
    clockpulsegenerator = clock_;
  }
  
  public int getQueryCount()
  {
    return query_count;
  }
  
  public double getQueryTime()
  {
    return total_query_time / query_count;
  }
  
  public int getJobCount()
  {
    return job_scheduled;
  }
}