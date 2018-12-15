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

  byte hashkey[];

  int    reqsend,rplyreceived;
  double searchrange[];

  RangeQuery queryset[];
  RangeQuery query;

  int    currentqueryno;
  double beginat;

  int cost;//cost of a dht query

  RandomAccessFile inputfile;
  RandomAccessFile timestamp;
  RandomAccessFile resultcount;
  RandomAccessFile result;
  RandomAccessFile querycost;

  FileWriter out;
  FileWriter query_time;

  int schedulecount;

  /** Creates a new instance of GridMonitorBroker */
  public GridMonitorBroker(String name_,int gridmonitoradminid_,String inputfile_) throws Exception 
  {
    super(name_);

    hashkey = new byte[20];

    gridmonitoradminid = gridmonitoradminid_;

    nodeid = this.get_id();

    clockpulsegenerator = new ClockPulseGenerator(this.nodeid);

    HashCode.compute(nodeid,hashkey);

    reqsend       = 0;
    rplyreceived  = 0;

    out = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_schedule_trace_"+this.nodeid+".dat");
    query_time = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_query_time_"+this.nodeid+".dat");

    System.out.println(inputfile_);
    
    inputfile   = new RandomAccessFile("./inputdata/lcg_arrival_abs.dat","r");
    timestamp   = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_timestamp.dat", "rw");
    resultcount = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_resultcount.dat", "rw");
    result      = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_result.dat", "rw");
    querycost   = new RandomAccessFile("./trace/" + GridMonitorTags.file + "/" + GridMonitorTags.file + "_querycost.dat", "rw");

    searchrange = new double[10];        

    searchrange[0] = 0.0;
    searchrange[1] = 0.5;
    searchrange[2] = 0.67;
    searchrange[3] = 0.75;
    searchrange[4] = 0.80;
    searchrange[5] = 0.83;
    searchrange[6] = 0.86;
    searchrange[7] = 0.88;
    searchrange[8] = 0.90;
    searchrange[9] = 1.00;

    queryset = new RangeQuery[6];

    beginat = 0.0;
    clock   = 0.0;

    schedulecount = 0;

    jobqueue = new ArrayList();

    //joblength=new double[10];
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

    //BufferedReader inputline=new BufferedReader(inputfile);

    try
    {
      do
      {
        this.send(gridmonitoradminid, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.ADD_BROKER, 
            new GridMonitorIO(this.nodeid, this.gridmonitoradminid, (Object)this.nodeid));

        this.getNextEvent(ev);

        //System.out.println("Event Tag "+ev.get_tag());
        sim_pause(2.0);
      }while(ev.get_tag() != GridMonitorTags.BROKER_ADDED);

      //System.out.println("Tag is"+ev.get_tag());
      isid = (Integer)(((GridMonitorIO)ev.get_data()).getdata());

      this.send(gridmonitoradminid, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.GET_A_FEEDBACK_NODE, 
          new GridMonitorIO(this.nodeid, this.gridmonitoradminid, null));

      this.getNextEvent(ev);

      feedbacknodeid = (Integer)((GridMonitorIO)ev.get_data()).getdata();

      //System.out.println("Broker is up now.....");   
      //System.out.println("Feed back node id is"+feedbackid);
      ev = null;

      //while(Sim_system.running())
      //this.sim_process(400.0);



      for(int i = 0; i < 6; i++)
      {

        switch (i)
        {
          case 0:
            HashCode.computeConsistentHash(searchrange[0],querystart);
            HashCode.computeConsistentHash(searchrange[2],queryend);            
            queryset[0] = new RangeQuery(querystart,queryend);                        
            break;

          case 1:
            HashCode.computeConsistentHash(searchrange[1],querystart);
            HashCode.computeConsistentHash(searchrange[3],queryend);            
            queryset[1] = new RangeQuery(querystart,queryend);
            break;

          case 2:
            HashCode.computeConsistentHash(searchrange[2],querystart);
            HashCode.computeConsistentHash(searchrange[3],queryend);            
            queryset[2] = new RangeQuery(querystart,queryend);
            break;

          case 3:
            HashCode.computeConsistentHash(searchrange[2],querystart);
            HashCode.computeConsistentHash(searchrange[5],queryend);            
            queryset[3] = new RangeQuery(querystart,queryend);
            break;

          case 4:
            HashCode.computeConsistentHash(searchrange[3],querystart);
            HashCode.computeConsistentHash(searchrange[2],queryend);            
            queryset[4] = new RangeQuery(querystart,queryend);
            break;

          case 5:
            HashCode.computeConsistentHash(searchrange[0],querystart);
            HashCode.computeConsistentHash(searchrange[4],queryend);            
            queryset[5] = new RangeQuery(querystart,queryend);
            break;
        }
        // System.out.println("Query "+i+" "+HashCode.getString(queryset[i].getStart())+" "+HashCode.getString(queryset[i].getEnd()));
      }

      uptime = Sim_system.clock();

      do
      {
        //this.sim_process(1.0);
        queryid = rand.nextInt(20);
        queryid = 377;

        //currentqueryno=(upduration<=100)?0:(upduration<=200)?1:(upduration<=500)?2:3;
        currentqueryno = rand.nextInt(6);
        //query=queryset[currentqueryno];
        query = queryset[5];
        //System.out.println("query is "+HashCode.getString(query.getStart())+" "+HashCode.getString(query.getEnd()));

        delay = GridSimRandom.real(60,.9,.9,rand.nextDouble());

        //System.out.println("delay for broker with node id "+this.nodeid+"is "+delay);
        size = -1;
        queuejobcount = 10 - jobqueue.size();

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

              System.out.println(time + " " + size * 120);
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

        this.sim_pause(20.0);

        cost = 0;

        indexQuery();

        //System.out.println("no of req send by "+this.nodeid+" "+(++reqsend));
        //System.out.println("sending ping message...");
        //this.sim_pause(10.0);
        //this.send(this.gridmonitoradminid,GridMonitorTags.SCHEDULE_NOW,GridMonitorTags.PING,new GridMonitorIO(this.nodeid,this.gridmonitoradminid,null));
        //beginat=clockpulsegenerator.getPulseCount();
        
        ev = new Sim_event();

        this.getNextEvent(ev);

        if(ev.get_src()!=-1) 
        {
          //System.out.println("Received event with tag"+ev.get_tag());
          processEvent(ev);
        }

        ev = null;

        downtime   = Sim_system.clock();
        upduration = downtime - uptime;
        //System.out.println("Uptime "+uptime+" Downtime "+downtime+" Upduration "+upduration);
      }while(clockpulsegenerator.isRunning());        

      System.out.println("broker finished...");

      out.close();
      query_time.close();
      timestamp.close();
      resultcount.close();
      result.close();
    }
    catch(Exception e)
    {

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

    switch(ev_.get_tag()) 
    {
      case GridMonitorTags.MORE_RESOURCE:
        while(!finish)
        {
          cost++;    

          tempres = (ArrayList)((ArrayList)(((GridMonitorIO)ev_.get_data()).getdata())).clone();

          if (tempres.size() != 0)
          {
            for(int j=0; j < tempres.size(); j++)
            {
              resourceid.add(tempres.get(j));
            }
          }

          this.getNextEvent(ev);

          ev_ = ev;

          if(ev_.get_tag() == GridMonitorTags.KEY_RESOURCE)
          {
            finish = true;                
          }
        }

      case GridMonitorTags.KEY_RESOURCE: 
        src     = ((GridMonitorIO)ev_.get_data()).getsrc();
        dest    = ((GridMonitorIO)ev_.get_data()).getdest();
        tempres = (ArrayList)((ArrayList)(((GridMonitorIO)ev_.get_data()).getdata())).clone();

        if (tempres.size() != 0)
        {
          for (int j=0; j < tempres.size(); j++)
          {
            resourceid.add(tempres.get(j));
          }
        }

        //querytime=Sim_system.clock();
        query_time.write(querytime + " " + Sim_system.clock() + " " + (Sim_system.clock() - querytime) + " to " + this.isid + "\n");                

        //store dht query cost
        querycost.writeBytes(cost + "\n");

        cost = 0;

        //if(resourceid.size()==0 )
        {
          if (clockpulsegenerator.isRunning() == true)
          {
            //this.sim_pause(10.0);
            //this.indexQuery();
          }
        }
        //else
        {
          schedule(resourceid);//schedule event ot newly arrived jobs
          //System.out.println("result set size "+resourceid.size());
          //ArrayList tempresourceid=(ArrayList)resourceid.clone();
          //Collections.shuffle(resourceid);
          tempres = null;
        }

        //System.out.println("Key held by node:"+resourceid);
        //System.out.println("--------------------------------------");
        //resulttime=Sim_system.clock();
        //IndexEntry entry;

        //System.out.println("Query is "+currentqueryno);
        resourceid = null;
        break;

      case GridMonitorTags.LOAD:
        temp = (gridmonitor.Accumulator)((GridMonitorIO)ev_.get_data()).getdata();
        System.out.println("Load at resource having id:" + ((GridMonitorIO)ev_.get_data()).getsrc() + 
            " is " + temp.getLast());
        break;

      case GridMonitorTags.KEY_NOT_FOUND:
        break;

      case GridMonitorTags.PONG:
        System.out.println("TAT for ping at " + this.nodeid + " response time " + 
            (clockpulsegenerator.getPulseCount() - beginat) + " send at " + beginat + " received at " + 
            clockpulsegenerator.getPulseCount());
    }

    // System.out.println("Finished at "+Sim_system.clock());
  }


  private void getNextEvent (Sim_event ev_)
  {
    boolean finish = false;

    ClockPulse pulse;

    while (!finish)
    {
      this.sim_get_next(ev_);

      if(ev_.get_tag() == GridMonitorTags.CLOCK_PULSE && Sim_system.running())
      {            
        pulse = (ClockPulse)(((GridMonitorIO)ev_.get_data()).getdata());
        clock = (clock + 1) % 100000;
        //System.out.println("Clock pulse:"+pulse.getPulseCount()+" received at "+this.nodeid);
      }
      else
      {       
        finish = true;
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
      Collections.shuffle(resourceid_);    

      IndexedValueComparator comp = new IndexedValueComparator();        

      Collections.sort(resourceid_, comp);

      System.out.println("__________________________________________________________\n" + 
          "Number of resources obtained:" + resourceid_.size() + " at " + Sim_system.clock());

      //out.write("Schedule number :"+schedulecount+"\n");
      schedulecount++;
      resultcount.writeBytes(Integer.toString(resourceid_.size())+"\n");

      for (i=0; i < resourceid_.size() && i < 20 && jobqueue.size() > 0; i++)
      {
        job = (GridJob)jobqueue.get(0);
        currentlength = (double)(job.getSize())*120; 

        if((job.getTime()) <= Sim_system.clock())
        {
          jobqueue.remove(0);

          gridlet1    = new Gridlet(i, currentlength, file_size, output_size);
          indexentry  = (IndexEntry)resourceid_.get(i);

          src = indexentry.getId();

          indexedload = indexentry.getLoad();

          this.send(src, GridMonitorTags.SCHEDULE_NOW, 
              GridSimTags.GRIDLET_SUBMIT, new GridMonitorIO(this.nodeid, src, gridlet1));

          this.getNextEvent(ev);

          load  = (Double)((GridMonitorIO)ev.get_data()).getdata();                                              
          resource = (Integer)((GridMonitorIO)ev.get_data()).getsrc();                        

          HashCode.computeConsistentHash(load, temphashkey);

          indexentry = new IndexEntry(load, temphashkey, resource);

          //sendFeedback(resource,indexentry,temphashkey);          
          out.write(Double.toString(currentlength) + "    " + Integer.toString(src) + 
              "   " + Double.toString(indexedload) + "   " + load + "   " + i +
              Sim_system.clock() + "\n");

          System.out.println(Double.toString(currentlength) + "    " + 
              Integer.toString(src) + "   " + Double.toString(indexedload) + 
              "   " + load + "   " + Sim_system.clock()+"\n");
        }
      }

      for (i = 0; i < resourceid_.size(); i++)
      {
        indexentry = (IndexEntry)resourceid_.get(i);
        result.writeBytes(Double.toString(Sim_system.clock()) + " " + Double.toString(indexentry.getLoad()) + "\n");            
      }

      //out.write("_______________________________________________________\n");
      timestamp.writeBytes(Double.toString(Sim_system.clock()) + "\n");
    }
    catch(Exception e)
    {
      System.out.println(e.toString() + " " + e.getMessage());
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

    super.send(this.feedbacknodeid, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.FIND_SUCCESSOR, new GridMonitorIO(this.nodeid, this.feedbacknodeid, hashkey));

    this.getNextEvent(ev);

    successor = ((GridMonitorIO)ev.get_data()).getsrc();
    feedbackrequest = new FeedbackRequest(resource, successor, indexentry_);

    super.send(this.feedbacknodeid, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.INDEX_FEEDBACK, new GridMonitorIO(this.nodeid, this.feedbacknodeid, feedbackrequest));

    //super.send(resource,GridMonitorTags.SCHEDULE_NOW,GridMonitorTags.REMOVE_INDEX_ENTRY,new GridMonitorIO(this.nodeid,resource,null));
    //super.send(this.feedbacknodeid,GridMonitorTags.SCHEDULE_NOW,GridMonitorTags.INDEX_FEEDBACK,new GridMonitorIO(this.nodeid,this.feedbacknodeid,feedbackrequest));
    //System.out.println("*********Feedback sent***********"+indexentry_.getLoad());
  }

  private void indexQuery()    
  {
    this.send(this.isid, GridMonitorTags.SCHEDULE_NOW, GridMonitorTags.KEY_LOOKUP, new GridMonitorIO(this.nodeid, this.isid, (Object)query));
    querytime = Sim_system.clock();
  }
}
