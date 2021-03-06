/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * $Id: TimeShared.java,v 1.39 2006/03/09 05:56:32 anthony Exp $
 */

package gridmonitor;

import java.util.*;
import java.util.Iterator;
import gridsim.*;
import eduni.simjava.*;


/**
 * TimeShared class is an allocation policy for GridResource that behaves
 * similar to a round robin algorithm, except that all Gridlets are
 * executed at the same time.
 * This is a basic and simple
 * scheduler that runs each Gridlet to one Processing Element (PE).
 * If a Gridlet requires more than one PE, then this scheduler only assign
 * this Gridlet to one PE.
 *
 * @author       Manzur Murshed and Rajkumar Buyya
 * @author       Anthony Sulistio (re-written this class)
 * @since        GridSim Toolkit 2.2
 * @see gridsim.GridSim
 * @see gridsim.ResourceCharacteristics
 * @invariant $none
 */
class TimeShared extends gridmonitor.AllocPolicy {
    private ResGridletList gridletInExecList_;  // storing exec Gridlets
    private ResGridletList gridletPausedList_;  // storing Paused Gridlets
    private double lastUpdateTime_;   // a timer to denote the last update time
    private MIShares share_;   // a temp variable
    
    ////////////////////////////////////////////////
    //int indexnode=-1;
    
    int update_count;
    
    boolean do_update;
    
    double currentvalue=0.0;
    double previousvalue=0.0;
    
    byte previousindexkey[]=new byte[20];
    byte indexkey[]=new byte[20];
    
    IndexEntry indexentry;
    boolean isupdating;
    
    boolean with_gridlet;
    boolean with_grupd;
    
    int update_received;
    
    int local_update_received;
    int remote_update_received;
    int local_update_send;
    int remote_update_send;
    int drop_count;
    
    Double deferredupdate=Double.NaN;
    
    int node_to_node_latency;
    
    HashCode hashcode;
       
    /**
     * Allocates a new TimeShared object
     * @param resourceName    the GridResource entity name that will contain
     *                        this allocation policy
     * @param entityName      this object entity name
     * @throws Exception This happens when one of the following scenarios occur:
     *      <ul>
     *          <li> creating this entity before initializing GridSim package
     *          <li> this entity name is <tt>null</tt> or empty
     *          <li> this entity has <tt>zero</tt> number of PEs (Processing
     *              Elements). <br>
     *              No PEs mean the Gridlets can't be processed.
     *              A GridResource must contain one or more Machines.
     *              A Machine must contain one or more PEs.
     *      </ul>
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre resourceName != null
     * @pre entityName != null
     * @post $none
     */
    TimeShared(String resourceName, String entityName,int gridmonitoradminid_,int nodeid_, ArrayList clocked_nodes, boolean with_gridlet_, boolean with_grupd_) throws Exception {
        super(resourceName, entityName,gridmonitoradminid_,nodeid_, clocked_nodes, with_gridlet_, with_grupd_);
        // initialises local data structure
        this.gridletInExecList_ = new ResGridletList();
        this.gridletPausedList_ = new ResGridletList();
        this.share_ = new MIShares();
        this.lastUpdateTime_ = 0.0;
        this.isupdating=false;
        int avg_hops = 1; 
        int latency_per_hop = 1; // Latency in micro seconds
        node_to_node_latency = avg_hops * latency_per_hop * 1;
        this.with_gridlet = with_gridlet_;
        this.with_grupd = with_grupd_;
        this.do_update = false;
        
        this.local_update_send = 0;
        this.remote_update_send = 0;
        //System.out.println("TimeShared myid " + this.myId_);
        
        hashcode = new HashCode();
        
        this.drop_count = 0;
    }
        
    ////////////////////// INTERNAL CLASS /////////////////////////////////
    
    /**
     * Gridlets MI share in Time Shared Mode
     */
    private class MIShares {
        /**  maximum amount of MI share Gridlets can get */
        public double max;
        
        /** minimum amount of MI share Gridlets can get when
         * it is executed on a PE that runs one extra Gridlet
         */
        public double min;
        
        /** Total number of Gridlets that get Max share */
        public int maxCount;
        
        /**
         * Default constructor that initializes all attributes to 0
         * @pre $none
         * @post $none
         */
        public MIShares() {
            max = 0.0;
            min = 0.0;
            maxCount = 0;
        }
    } // end of internal class
    
    /////////////////////// End of Internal Class /////////////////////////
    
    /**
     * Handles internal events that are coming to this entity.
     * @pre $none
     * @post $none
     */
    
    public void body() {
        
        update_count=0;
        int src;
        //byte hashkey[]=new byte[20];
        //double newvalue;
        ////////////////////////////////////////////////               
        Sim_event ev;        
        /////////////////////////////////////////////////
        
        
        //System.out.println("My id"+this.myId_);       
        //this.send(this.myId_,node_to_node_latency,GridMonitorTags.UPDATE_INDEX,new GridMonitorIO(this.myId_,0,(Object)));
        //super.indexCurrentLoad();               
        //this.addTotalLoad(0.0);
        
        //a loop that is looking for internal events only
        while (Sim_system.running() && endSimulation_ == false) {
            
            //if ((Sim_system.clock() % 10 == 0) && (gridletInExecList_.size() > 0))
            //{
            //  this.addTotalLoad(0.0);
            //}
            
            ev = new Sim_event();
            
            super.sim_get_next(ev);
            
            //System.out.println("Tag: "+ev.get_tag());
            
            //if(Sim_system.running())
            {
                //System.out.println("*******Event tag: "+ev.get_tag()+"from node"+ev.get_src()+" "+ev.get_dest());
                // if the simulation finishes then exit the loop
                if (ev.get_tag() == GridSimTags.END_OF_SIMULATION ||super.isEndSimulation() == true) {
                    break;
                }
                
                // Internal Event if the event source is this entity
                
                switch(ev.get_tag()) {
                    case GridMonitorTags.UPDATE_INDEX:
                        previousvalue=currentvalue;
                        currentvalue=(Double)((GridMonitorIO)ev.get_data()).getdata();
                        
                        //System.out.println("Updateing index " + currentvalue);
                        
                        startUpdate(currentvalue);
                        
                        break;
                    case GridMonitorTags.A_INDEX_NODE:
                        src=(Integer)(((GridMonitorIO)ev.get_data()).getdata());
                      
                        //System.out.println("Received an index node in time shared " + this.myId_ + " from " + src);
                                                
                        this.send(src,node_to_node_latency,GridMonitorTags.FIND_SUCCESSOR,new GridMonitorIO(this.myId_,src,(Object)indexkey, false));
                        
                        break;
                    case GridMonitorTags.SUCCESSOR:                      
                        
                       if (indexnode != -1) {
                          indexentry=new IndexEntry(previousvalue,previousindexkey,this.nodeid);
                          //System.out.println(update_count+":Sending remove request.. by "+this.nodeid+" "+hashcode.getString(previousindexkey));
                          this.send(indexnode,node_to_node_latency,GridMonitorTags.REMOVE,new GridMonitorIO(this.myId_,indexnode,(Object)indexentry, false));           
                        } 
                        
                        
                        src=(Integer)(((GridMonitorIO)ev.get_data()).getdata());
                        
                        //System.out.println("[TIME SHARED INDEX NODE]Index node " + indexnode);
                        
                        indexentry=new IndexEntry(currentvalue,indexkey,this.nodeid);
                        this.send(src,node_to_node_latency,GridMonitorTags.INDEX,new GridMonitorIO(this.myId_,src,(Object)indexentry, false));
                                                
                        /*
                        if (currentvalue > 0.0)
                        {
                          System.out.println("[TIME SHARED] Load updated to " + hashcode.getString(indexkey) + " for " + this.nodeid + " to value " + currentvalue);
                        }
                        */
                        break;
                    case GridMonitorTags.INDEX_UPDATED:
                        src=(Integer)(((GridMonitorIO)ev.get_data()).getsrc());
                        indexnode=src;
                        previousvalue=currentvalue;
                        
                        isupdating=false;
                        
                        //System.out.println(update_count+":value updated by "+this.nodeid+" to "+hashcode.getString(indexkey));
                        if (Double.isNaN(deferredupdate)==false) {
                            //this.send(this.myId_,node_to_node_latency,GridMonitorTags.UPDATE_INDEX,new GridMonitorIO(this.myId_,this.myId_,(Object)deferredupdate));
                            // System.out.println("Updateing deferred index " + deferredupdate);
                            
                            startUpdate(deferredupdate);
                            deferredupdate=Double.NaN;
                            
                        }
                        break;
                        
                    case GridMonitorTags.REMOVED:
                        //System.out.println(update_count+":removed entry "+hashcode.getString(previousindexkey)+" for "+this.nodeid);
                        //this.send(this.gridmonitoradminid,node_to_node_latency,GridMonitorTags.GET_A_INDEX_NODE,new GridMonitorIO(this.myId_,this.gridmonitoradminid,null));                                  
                        break;                        
                    
                    default:
                        if (ev.get_src() == super.myId_) {
                           //System.out.println("Internal event " + ev.get_tag()); 
                           internalEvent();
                        }
                        break;
                }
                
            }
            
            ev=null;
            
            //sim_pause(20);
        }
        
        // CHECK for ANY INTERNAL EVENTS WAITING TO BE PROCESSED
        
        while (super.sim_waiting() > 0) {
            ev=new Sim_event();
            // wait for event and ignore since it is likely to be related to
            // internal event scheduled to update Gridlets processing
            super.sim_get_next(ev);
            System.out.println(super.resName_ +
                    ".TimeShared.body(): ignoring internal events");
            ev=null;
        }        
    }
    
    
    private void startUpdate(double newvalue) {
        
        if(isupdating==false && endSimulation_ == false) {
            ++update_count;
            //isupdating=true;   
            
            currentvalue = newvalue;
            //System.out.println("Computing hash for " + previousvalue + " " + currentvalue);
            
            hashcode.computeConsistentHash(previousvalue,previousindexkey);
            hashcode.computeConsistentHash(currentvalue, indexkey);
            
            //System.out.println("Starting update for new load");
            //System.out.println("Current load = " + currentvalue + " index key = " + hashcode.getString(indexkey));
            
            /*
            if (indexnode != -1) {
                indexentry=new IndexEntry(previousvalue,previousindexkey,this.nodeid);
                //System.out.println(update_count+":Sending remove request.. by "+this.nodeid+" "+hashcode.getString(previousindexkey));
                this.send(indexnode,node_to_node_latency,GridMonitorTags.REMOVE,new GridMonitorIO(this.myId_,indexnode,(Object)indexentry, false));           
            } 
            else 
            */
            {
                this.send(this.gridmonitoradminid, node_to_node_latency, GridMonitorTags.GET_A_INDEX_NODE, 
                    new GridMonitorIO(this.myId_,this.gridmonitoradminid,null, false));                
            }
            
        } else {
            deferredupdate=newvalue;
            //System.out.println("deffered value..."+deferredupdate);
        }
    }
    
    
    /**
     * Schedules a new Gridlet that has been received by the GridResource
     * entity.
     * @param   gl    a Gridlet object that is going to be executed
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gl != null
     * @post $none
     */
    public void gridletSubmit(Gridlet gl, int src, boolean ack, boolean do_update, boolean is_local) {
        // update Gridlets in execution up to this point in time
        
        updateGridletProcessing(do_update);
        
        this.do_update = do_update;
        
        /*
        if (is_local == true)
        {
         System.out.println("[TIME SHARED][SUBMIT LOCAL] Gridlet of size " + gl.getGridletLength() + " submitted");
        }
        else
        {
          System.out.println("[TIME SHARED][SUBMIT REMOTE] Gridlet of size " + gl.getGridletLength() + " submitted");
        }
        */
        // reset number of PE since at the moment, it is not supported
        if (gl.getNumPE() > 1) {
            String userName = GridSim.getEntityName( gl.getUserID() );
            System.out.println();
            System.out.println(super.get_name() + ".gridletSubmit(): " +
                    " Gridlet #" + gl.getGridletID() + " from " + userName +
                    " user requires " + gl.getNumPE() + " PEs.");
            System.out.println("--> Process this Gridlet to 1 PE only.");
            System.out.println();
            
            // also adjusted the length because the number of PEs are reduced
            int numPE = gl.getNumPE();
            double len = gl.getGridletLength();
            gl.setGridletLength(len*numPE);
            gl.setNumPE(1);
        }
        double load;
        
        load = 0.0;
        
        // adds a Gridlet to the in execution list
        //if ((is_local == true && gridletInExecList_.size() < 16) || is_local == false)
        {
        gridmonitor.ResGridlet rgl = new gridmonitor.ResGridlet(gl);
        rgl.setGridletStatus(Gridlet.INEXEC); // set the Gridlet status to exec
        gridletInExecList_.add(rgl);   // add into the execution list
        
        //System.out.println("Gridlet execution list size is " + gridletInExecList_.size() + " core " + this.nodeid + " cycle " + Sim_system.clock());
        
        if (gridletInExecList_.size() > 16 && is_local == false)
        {
          //System.out.println("Gridlet dropped with size " + gridletInExecList_.size());
          submit_dropped += 1;
        }
        else if (gridletInExecList_.size() > 16)
        {
          //System.out.println("Gridlet local dropped with size " + gridletInExecList_.size());
          
          submit_dropped += 1;
        }
        
        load = calculateTotalLoad(gridletInExecList_.size());
        
        // HACK: to not to submit a gridlet
        //if (is_local == false && (with_gridlet == true || do_update == true))
        if (with_gridlet == true)
        {
          super.addTotalLoad(load, false);          
          
          local_update_send += 1;
          //System.out.println("Total update send " + local_update_send);
          //System.out.println("[TIME SHARED] Gridlet submitted at node " + this.nodeid + " at " + Sim_system.clock());
        } 
        else
        {  
          //else if (load > 0.0)
          
          if (is_local == true)
          {
            local_update_received += 1;
          
            if (local_update_received > 16)
            //if (do_update == true || local_update_received == 1)
            //if (this.do_update == true)
            {
              
              
              indexentry=new IndexEntry(currentvalue, indexkey, this.nodeid);
              //System.out.println("At node " + this.myId_ + " indexing " + hashcode.getString(indexkey) + " value " + currentvalue + " at " + indexnode);
              
              super.addTotalLoad(load, false);
              //this.send(indexnode, node_to_node_latency * this.myId_, GridMonitorTags.INDEX, new GridMonitorIO(this.myId_, indexnode, (Object)indexentry, false));
              local_update_received = 0;
            
              local_update_send += 1;
            
              // System.out.println("Local update send " + local_update_send);
            }
          }
          else
          {
            remote_update_received += 1;
          
            if (with_grupd == true)
            {
              if (do_update == true)
              {
                indexentry=new IndexEntry(currentvalue, indexkey, this.nodeid);
                //System.out.println("At node " + this.myId_ + " indexing " + hashcode.getString(indexkey) + " value " + currentvalue + " at " + indexnode);          
                super.addTotalLoad(load, false);
              
                //this.send(indexnode, node_to_node_latency * this.myId_, GridMonitorTags.INDEX, new GridMonitorIO(this.myId_, indexnode, (Object)indexentry, false));
                remote_update_received = 0;
                remote_update_send += 1;
                //System.out.println("Remote update send " + remote_update_send);
              }
            }
            else if (remote_update_received > 16)            
            {
              indexentry=new IndexEntry(currentvalue, indexkey, this.nodeid);
              //System.out.println("At node " + this.myId_ + " indexing " + hashcode.getString(indexkey) + " value " + currentvalue + " at " + indexnode);          
              super.addTotalLoad(load, false);
              
              //this.send(indexnode, node_to_node_latency * this.myId_, GridMonitorTags.INDEX, new GridMonitorIO(this.myId_, indexnode, (Object)indexentry, false));
              remote_update_received = 0;
              remote_update_send += 1;
              //System.out.println("Remote update send " + remote_update_send);
            }
          }          
          
          //if (do_update == true)
          //{
            //super.addTotalLoad(load);
          //}
        }
        }
        
        //if (is_local == true)
        {          
          update_received += 1;
          if (do_update == true)
          {
            //super.addTotalLoad(load);
          }
          
         /*
          if (update_received > 16)
          {
            indexentry=new IndexEntry(currentvalue, indexkey, this.nodeid);
            //System.out.println("At node " + this.myId_ + " indexing " + hashcode.getString(indexkey) + " value " + currentvalue + " at " + indexnode);
            this.send(indexnode, node_to_node_latency * this.myId_, GridMonitorTags.INDEX, new GridMonitorIO(this.myId_, indexnode, (Object)indexentry, false));
            update_received = 0;
          }
          */
        } 
               
        if (load > 0.0 && load <= 1.0 && ack == true)
        {
          submit_successful += 1;
          //System.out.println("Load while submitting gridlet " + load);
        }
       
        // sends back an ack if required
        if (ack == true)         
        {
            //this.send(src, GridMonitorTags.SCHEDULE_IMM, GridSimTags.GRIDLET_SUBMIT_ACK,
            //    new GridMonitorIO(this.nodeid, src, load, false));
            //System.out.println("Ack sent for gridlet submission to node " + src + 
            //    " from node " + this.nodeid + " with load " + load);
        }
        
        //System.out.println("Gridlet submitted..."+gl.getGridletFinishedSoFar());
        // forecast all Gridlets in the execution list
        
        forecastGridlet();
    }
    
    /**
     * Finds the status of a specified Gridlet ID.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @return the Gridlet status or <tt>-1</tt> if not found
     * @see gridsim.Gridlet
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public int gridletStatus(int gridletId, int userId) {
        ResGridlet rgl = null;
        
        // Find in EXEC List first
        int found = super.findGridlet(gridletInExecList_, gridletId, userId);
        if (found >= 0) {
            // Get the Gridlet from the execution list
            rgl = (ResGridlet) gridletInExecList_.get(found);
            return rgl.getGridletStatus();
        }
        
        // if not found then find again in Paused List
        found = super.findGridlet(gridletPausedList_, gridletId, userId);
        if (found >= 0) {
            // Get the Gridlet from the execution list
            rgl = (ResGridlet) gridletPausedList_.get(found);
            return rgl.getGridletStatus();
        }
        
        // if not found in all lists
        return -1;
    }
    
    /**
     * Cancels a Gridlet running in this entity.
     * This method will search the execution and paused list. The User ID is
     * important as many users might have the same Gridlet ID in the lists.
     * <b>NOTE:</b>
     * <ul>
     *    <li> Before canceling a Gridlet, this method updates all the
     *         Gridlets in the execution list. If the Gridlet has no more MIs
     *         to be executed, then it is considered to be <tt>finished</tt>.
     *         Hence, the Gridlet can't be canceled.
     *
     *    <li> Once a Gridlet has been canceled, it can't be resumed to
     *         execute again since this method will pass the Gridlet back to
     *         sender, i.e. the <tt>userId</tt>.
     *
     *    <li> If a Gridlet can't be found in both execution and paused list,
     *         then a <tt>null</tt> Gridlet will be send back to sender,
     *         i.e. the <tt>userId</tt>.
     * </ul>
     *
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public void gridletCancel(int gridletId, int userId) {
        // Finds the gridlet in execution and paused list
        ResGridlet rgl = cancel(gridletId, userId);
        
        // If not found in both lists then report an error and sends back
        // an empty Gridlet
        if (rgl == null) {
            //System.out.println(super.resName_ +
            //        ".TimeShared.gridletCancel(): Cannot find " +
            //        "Gridlet #" + gridletId + " for User #" + userId);
            
            super.sendCancelGridlet(GridSimTags.GRIDLET_CANCEL, null,
                    gridletId, userId);
            return;
        }
        
        // if a Gridlet is found
        rgl.finalizeGridlet();     // finalise Gridlet
        
        // if a Gridlet has finished execution before canceling, the reports
        // an error msg
        if (rgl.getGridletStatus() == Gridlet.SUCCESS) {
            //System.out.println(super.resName_
            //        + ".TimeShared.gridletCancel(): Cannot cancel"
            //        + " Gridlet #" + gridletId + " for User #" + userId
            //        + " since it has FINISHED.");
        }
        
        // sends the Gridlet back to sender
        super.sendCancelGridlet(GridSimTags.GRIDLET_CANCEL, rgl.getGridlet(),
                gridletId, userId);
    }
    
    /**
     * Pauses a Gridlet only if it is currently executing.
     * This method will search in the execution list. The User ID is
     * important as many users might have the same Gridlet ID in the lists.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public void gridletPause(int gridletId, int userId, boolean ack) {
        boolean status = false;
        
        // find this Gridlet in the execution list
        int found = super.findGridlet(gridletInExecList_, gridletId, userId);
        if (found >= 0) {
            // update Gridlets in execution list up to this point in time
            updateGridletProcessing(false);
            
            // get a Gridlet from execution list
            ResGridlet rgl = (ResGridlet) gridletInExecList_.remove(found);
            
            // if a Gridlet is finished upon pausing, then set it to success
            // instead.
            if (rgl.getRemainingGridletLength() == 0.0) {
                //System.out.println(super.resName_
                //        + ".TimeShared.gridletPause(): Cannot pause"
                //        + " Gridlet #" + gridletId + " for User #" + userId
                //        + " since it is FINISHED.");
                
                gridletFinish(rgl, Gridlet.SUCCESS);
            } else {
                status = true;
                rgl.setGridletStatus(Gridlet.PAUSED);
                
                // add the Gridlet into the paused list
                gridletPausedList_.add(rgl);
                //System.out.println(super.resName_ +
                //        ".TimeShared.gridletPause(): Gridlet #" + gridletId +
                //        " with User #" + userId + " has been sucessfully PAUSED.");
            }
            
            // forecast all Gridlets in the execution list
            forecastGridlet();
        } else   // if not found in the execution list
        {
            //System.out.println(super.resName_ +
            //        ".TimeShared.gridletPause(): Cannot find " +
            //        "Gridlet #" + gridletId + " for User #" + userId);
        }
        
        // sends back an ack
        if (ack == true) {
            super.sendAck(GridSimTags.GRIDLET_PAUSE_ACK, status,
                    gridletId, userId);
        }
    }
    
    /**
     * Moves a Gridlet from this GridResource entity to a different one.
     * This method will search in both the execution and paused list.
     * The User ID is important as many Users might have the same Gridlet ID
     * in the lists.
     * <p>
     * If a Gridlet has finished beforehand, then this method will send back
     * the Gridlet to sender, i.e. the <tt>userId</tt> and sets the
     * acknowledgment to false (if required).
     *
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @param destId       a new destination GridResource ID for this Gridlet
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gridletId > 0
     * @pre userId > 0
     * @pre destId > 0
     * @post $none
     */
    public void gridletMove(int gridletId, int userId, int destId, boolean ack) {
        // cancel the Gridlet first
        ResGridlet rgl = cancel(gridletId, userId);
        
        // If no found then print an error msg
        if (rgl == null) {
            System.out.println(super.resName_ +
                    ".TimeShared.gridletMove(): Cannot find " +
                    "Gridlet #" + gridletId + " for User #" + userId);
            
            if (ack == true)   // sends ack that this operation fails
            {
                super.sendAck(GridSimTags.GRIDLET_SUBMIT_ACK, false,
                        gridletId, userId);
            }
            return;
        }
        
        // if found
        rgl.finalizeGridlet();   // finalise Gridlet
        Gridlet gl = rgl.getGridlet();
        
        // if a Gridlet has finished execution
        if (gl.getGridletStatus() == Gridlet.SUCCESS) {
            System.out.println(super.resName_
                    + ".TimeShared.gridletMove(): Cannot move"
                    + " Gridlet #" + gridletId + " for User #" + userId
                    + " since it has FINISHED.");
            
            if (ack == true) {
                super.sendAck(GridSimTags.GRIDLET_SUBMIT_ACK, false, gridletId,
                        userId);
            }
            
            super.sendFinishGridlet(gl);   // sends the Gridlet back to sender
        }
        // moves this Gridlet to another GridResource entity
        else {
            super.gridletMigrate(gl, destId, ack);
        }
    }
    
    /**
     * Resumes a Gridlet only in the paused list.
     * The User ID is important as many Users might have the same Gridlet ID
     * in the lists.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @param   ack   an acknowledgement, i.e. <tt>true</tt> if wanted to know
     *        whether this operation is success or not, <tt>false</tt>
     *        otherwise (don't care)
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    public void gridletResume(int gridletId, int userId, boolean ack) {
        boolean success = false;
        
        // finds in the execution list first
        int found = super.findGridlet(gridletPausedList_, gridletId, userId);
        if (found >= 0) {
            // need to update Gridlets in execution up to this point in time
            updateGridletProcessing(false);
            
            // remove a Gridlet from paused list and change the status
            ResGridlet rgl = (ResGridlet) gridletPausedList_.remove(found);
            rgl.setGridletStatus(Gridlet.RESUMED);
            
            // add the Gridlet back to in execution list
            gridletInExecList_.add(rgl);
            
            // then forecast Gridlets in execution list
            forecastGridlet();
            
            success = true;
            System.out.println(super.resName_ +
                    ".TimeShared.gridletResume(): Gridlet #" + gridletId +
                    " with User #" + userId + " has been sucessfully RESUMED.");
        } else  // if no found then prints an error msg
        {
            System.out.println(super.resName_ +
                    ".TimeShared.gridletResume(): Cannot find Gridlet #" +
                    gridletId + " for User #" + userId);
        }
        
        // sends back an ack to sender
        if (ack == true) {
            super.sendAck(GridSimTags.GRIDLET_RESUME_ACK, success,
                    gridletId, userId);
        }
    }    
    
    ////////////////////// PRIVATE METHODS //////////////////////////////
    
    /**
     * Updates the execution of all Gridlets for a period of time.
     * The time period is determined from the last update time up to the
     * current time. Once this operation is successfull, then the last update
     * time refers to the current time.
     * @pre $none
     * @post $none
     */
    private void updateGridletProcessing(boolean do_update) {
        // Identify MI share for the duration (from last event time)
        double time = Sim_system.clock();
        double timeSpan = time - lastUpdateTime_;
        //System.out.println("clock.."+Sim_system.clock());
        // if current time is the same or less than the last update time,
        // then ignore
        if (timeSpan <= 0.0) {
            return;
        }
        
        // Update Current Time as the Last Update
        lastUpdateTime_ = time;
        
        // update the GridResource load
        int size = gridletInExecList_.size();
        
        double load = super.calculateTotalLoad(size);
        
        // HACK    
        //if (with_gridlet == true || do_update == true)
        //if (with_gridlet == true)
        {
          //super.addTotalLoad(load);       // add the current resource load
        }
        
        // if no Gridlets in execution then ignore the rest
        if (size == 0) {
            return;
        }
        
        // gets MI Share for all Gridlets
        
        MIShares shares = getMIShare(timeSpan, size);
        ResGridlet obj = null;
        
        // a loop that allocates MI share for each Gridlet accordingly
        // In this algorithm, Gridlets at the front of the list
        // (range = 0 until MIShares.maxCount-1) will be given max MI value
        // For example, 2 PEs and 3 Gridlets. PE #0 processes Gridlet #0
        // PE #1 processes Gridlet #1 and Gridlet #2
        int i = 0;  // a counter
        try
        {
          Iterator iter = gridletInExecList_.iterator();
          while ( iter.hasNext() ) {
            obj = (ResGridlet) iter.next();
            
            // Updates the Gridlet length that is currently being executed
            if (i < shares.maxCount) {
                
                obj.updateGridletFinishedSoFar(shares.max);
                //obj.updateGridletFinishedSoFar(10);
            } else {
                
                obj.updateGridletFinishedSoFar(shares.min);
                //obj.updateGridletFinishedSoFar(10);
            }
            
            //System.out.println("Gritlet finished for node " + this.nodeid + " with jobs " + gridletInExecList_.size() + " " + shares.max + " " + Sim_system.clock() + " "+ obj.getGridletLength());
            
            i++;   // increments i
          }
        }
        catch (Exception e)
        {
          System.out.println("Exception at 637");
        }
    }
    
    /**
     * Identifies MI share (max and min) for all Gridlets in
     * a given time duration
     * @param timeSpan duration
     * @param size    total number of Gridlets in the execution list
     * @return  the total MI share that a Gridlet gets for a given
     *          <tt>timeSpan</tt>
     */
    private MIShares getMIShare(double timeSpan, int size) {
        // 1 - localLoad_ = available MI share percentage
        double localLoad = super.resCalendar_.getCurrentLoad();
        double TotalMIperPE = super.resource_.getMIPSRatingOfOnePE() * timeSpan
                * (1 - localLoad);
        
        // This TimeShared is not Round Robin where each PE for 1 Gridlet only.
        // a PE can have more than one Gridlet executing.
        // minimum number of Gridlets that each PE runs.
        int glDIVpe = size / super.totalPE_;
        
        // number of PEs that run one extra Gridlet
        int glMODpe = size % super.totalPE_;
        
        //System.out.println(size+" "+glDIVpe+" "+glMODpe);
        // If num Gridlets in execution > total PEs in a GridResource,
        // then divide MIShare by the following constraint:
        // - obj.max = MIShare of a PE executing n Gridlets
        // - obj.min = MIShare of a PE executing n+1 Gridlets
        // - obj.maxCount = a threshold number of Gridlets will be assigned to
        //                  max MI value.
        //
        // In this algorithm, Gridlets at the front of the list
        // (range = 0 until maxCount-1) will be given max MI value
        if (glDIVpe > 0) {
            // this is for PEs that run one extra Gridlet
            share_.min = TotalMIperPE / (glDIVpe + 1);
            share_.max = TotalMIperPE / glDIVpe;
            share_.maxCount = (super.totalPE_ - glMODpe) * glDIVpe;
        }
        
        // num Gridlet in Exec < total PEs, meaning it is a
        // full PE share: i.e a PE is dedicated to execute a single Gridlet
        else {
            share_.max = TotalMIperPE;
            share_.min = TotalMIperPE;
            share_.maxCount = size;   // number of Gridlet
        }
        
        //System.out.println("Share for node " + this.nodeid + " " + share_.maxCount + " " + share_.max + " " + share_.min);
        
        return share_;
    }
    
    /**
     * Determines the smallest completion time of all Gridlets in the execution
     * list. The smallest time is used as an internal event to
     * update Gridlets processing in the future.
     * <p>
     * The algorithm for this method:
     * <ul>
     *     <li> identify the finish time for each Gridlet in the execution list
     *          given the share MIPS rating for all and the remaining Gridlet's
     *          length
     *     <li> find the smallest finish time in the list
     *     <li> send the last Gridlet in the list with
     *          <tt>delay =  smallest finish time - current time</tt>
     * </ul>
     * @pre $none
     * @post $none
     */
    private void forecastGridlet() {
        // if no Gridlets available in exec list, then exit this method
        if (gridletInExecList_.size() == 0) {
            return;
        }
         
        // checks whether Gridlets have finished or not. If yes, then remove
        // them since they will effect the MIShare calculation.
        //System.out.println("****"+gridletInExecList_.size());
        
        checkGridletCompletion();
        
        // Identify MIPS share for all Gridlets for 1 second, considering
        // current Gridlets + No of PEs.
        
        MIShares share = getMIShare(1.0, gridletInExecList_.size());
        
        ResGridlet rgl = null;
        
        int i = 0;
        int gl_count = 0;
        
        double time         = 0.0;
        double rating       = 0.0;
        double smallestTime = 0.0;
        
        // For each Gridlet, determines their finish time
        //Iterator iter = gridletInExecList_.iterator();
        //while ( iter.hasNext() ) 
        while (i < gridletInExecList_.size()) 
        {
          try
          {
            rgl = (ResGridlet) gridletInExecList_.get(i);
            
            // If a Gridlet locates before the max count then it will be given
            // the max. MIPS rating
            if (i < share.maxCount) 
            {
                rating = share.max;
            } 
            else 
            {   // otherwise, it will be given the min. MIPS Rating
                rating = share.min;
            }
            
            time = forecastFinishTime(rating, rgl.getRemainingGridletLength() );
            
            int roundUpTime = (int) (time + 1);   // rounding up
            
            //System.out.println("#############"+roundUpTime);
            rgl.setFinishTime(roundUpTime);
            
            // get the smallest time of all Gridlets
            if (i == 0 || smallestTime > time) 
            {
                smallestTime = time;
            }
            
            i++;            
          }
          catch (Exception e)
          {
            System.out.println("Exception in forcast gridlet for index " + i);
          }
        }
          
        gl_count++;
      
        
        // sends to itself as an internal event
        // System.out.println(gridletInExecList_.size()+"next bust at :"+smallestTime);
        // super.sendInternalEvent(smallestTime);
        super.sendInternalEvent(1.0);
        //super.sendInternalEvent(node_to_node_latency);
    }
    
    /**
     * Checks all Gridlets in the execution list whether they are finished or
     * not.
     * @pre $none
     * @post $none
     */
    private void checkGridletCompletion() 
    {
        ResGridlet rgl = null;
        //System.out.println(gridletInExecList_.size());
        // a loop that determine the smallest finish time of a Gridlet
        // Don't use an iterator since it causes an exception because if
        // a Gridlet is finished, gridletFinish() will remove it from the list.
        
        int i = 0;
        
        while ( i < gridletInExecList_.size() ) 
        {
            rgl = (ResGridlet) gridletInExecList_.get(i);
            // if a Gridlet has finished, then remove it from the list
            if (rgl.getRemainingGridletLength() <= 0.0) 
            {
                gridletFinish(rgl, Gridlet.SUCCESS);
                //System.out.println("[TIME SHARED] Gridlet finished at node " + this.nodeid + " at " + Sim_system.clock());
                continue;  // not increment i coz the list size also decreases
            }
            
            i++;
        }
    }
    
    /**
     * Forecast finish time of a Gridlet.
     * <tt>Finish time = length / available rating</tt>
     * @param availableRating   the shared MIPS rating for all Gridlets
     * @param length   remaining Gridlet length
     * @return Gridlet's finish time.
     */
    private double forecastFinishTime(double availableRating, double length) {
        double finishTime = length / availableRating;
        //double finishTime=length-1.0;
        // This is as a safeguard since the finish time can be extremely
        // small close to 0.0, such as 4.5474735088646414E-14. Hence causing
        // some Gridlets never to be finished and consequently hang the program
        if (finishTime < 1.0) {
            finishTime = 1.0;
        }
        
        return finishTime;
    }
    
    /**
     * Updates the Gridlet's properties, such as status once a
     * Gridlet is considered finished.
     * @param rgl     a ResGridlet object
     * @param status  the status of this ResGridlet object
     * @pre rgl != null
     * @post $none
     */
    private void gridletFinish(ResGridlet rgl, int status) {
        // NOTE: the order is important! Set the status first then finalize
        // due to timing issues in ResGridlet class.
        rgl.setGridletStatus(status);
        rgl.finalizeGridlet();
        
        // sends back the Gridlet with no delay
        //Gridlet gl = rgl.getGridlet();
        //super.sendFinishGridlet(gl);
        
        //System.out.println("Gritlet finished for node " + this.nodeid + " with jobs " + gridletInExecList_.size() + " " + Sim_system.clock() + " "+ rgl.getGridletLength());
        
        // remove this Gridlet in the execution
        gridletInExecList_.remove(rgl);
    }
    
    /**
     * Handles internal event
     * @pre $none
     * @post $none
     */
    private void internalEvent() {
        // this is a constraint that prevents an infinite loop
        // Compare between 2 floating point numbers. This might be incorrect
        // for some hardware platform.
        // System.out.println("Received internal event at " + Sim_system.clock());
        
        if ( lastUpdateTime_ == Sim_system.clock() ) {
            return;
        }
        
        // update Gridlets in execution up to this point in time
        //System.out.println("[TIME SHARED] Inernal event");
        
        updateGridletProcessing(true);
        
        // update the GridResource load
        int size = gridletInExecList_.size();
        double load = super.calculateTotalLoad(size);
        
        // HACK    
        //if (with_gridlet == true || do_update == true)
        if (with_gridlet == true || this.do_update == true)
        {
          //
          
          //super.addTotalLoad(load);       // add the current resource load
          this.do_update = false;
        }
        
        // schedule next event
        forecastGridlet();
    }
    
    /**
     * Handles an operation of canceling a Gridlet in either execution list
     * or paused list.
     * @param gridletId    a Gridlet ID
     * @param userId       the user or owner's ID of this Gridlet
     * @param an object of ResGridlet or <tt>null</tt> if this Gridlet is not
     *        found
     * @pre gridletId > 0
     * @pre userId > 0
     * @post $none
     */
    private ResGridlet cancel(int gridletId, int userId) {
        ResGridlet rgl = null;
        
        // Check whether the Gridlet is in execution list or not
        int found = super.findGridlet(gridletInExecList_, gridletId, userId);
        
        // if a Gridlet is in execution list
        if (found >= 0) {
            // update the gridlets in execution list up to this point in time
            updateGridletProcessing(false);
            
            //System.out.println("Gridlet finished");
            
            // Get the Gridlet from the execution list
            rgl = (ResGridlet) gridletInExecList_.remove(found);
            
            // if a Gridlet is finished upon cancelling, then set it to success
            if (rgl.getRemainingGridletLength() == 0.0) {
                rgl.setGridletStatus(Gridlet.SUCCESS);
            } else {
                rgl.setGridletStatus(Gridlet.CANCELED);
            }
            
            // then forecast the next Gridlet to complete
            forecastGridlet();
        }
        
        // if a Gridlet is not in exec list, then find it in the paused list
        else {
            found = super.findGridlet(gridletPausedList_, gridletId, userId);
            
            // if a Gridlet is found in the paused list then remove it
            if (found >= 0) {
                rgl = (ResGridlet) gridletPausedList_.remove(found);
                rgl.setGridletStatus(Gridlet.CANCELED);
            }
        }
        
        return rgl;
    }    
        
} // end class