/*
 * GridMonitorIO.java
 *
 * Created on January 2, 2003, 1:33 AM
 *
 * @author Siddharth Rai
 *
 */

package gridmonitor;

import java.util.*;

public class GridMonitorIO 
{
  private int src,dest;
  private Object data;

  /** Creates a new instance of GridMonitorIO */
  public GridMonitorIO(int src_,int dest_,Object data_) {
    src=src_;
    dest=dest_;
    data=data_;
  }
  public Object getdata()
  {
    return data;
  }
  public int getsrc()
  {
    return src;
  }

  public int getdest()
  {
    return dest;
  }
}

class FingerEntry
{
 private byte[] hashkey;
 private int id;
 FingerEntry()
 {
     hashkey=new byte[20];
 }
 public byte[] getHashKey()
 {
  return hashkey;
 }
 public int getId()
 {
  return id;   
 }
}

class SuccessorList
{
    private byte start[];
    private byte end[];
    public SuccessorList(byte[] start_,byte[]end_)
    {
        start=(byte[])start_.clone();
        end=(byte[])end_.clone();
        /*int i;
        for(i=0;i<start_.length;start[i]=start_[i],i++);
        for(i=0;i<end_.length;end[i]=end_[i],i++);
         */       
    }
    public void getStart(byte[]start_)
    {
        int i;
        for(i=0;i<start_.length;start_[i]=start[i],i++);         
    }
    
    public void getEnd(byte[]end_)
    {
        int i;
        for(i=0;i<end_.length;end_[i]=end[i],i++);
       
    }
}



class JoiningMessage
{
    private ArrayList successorlist;
    private int predecessor;
    private int successor;
    public JoiningMessage(ArrayList successorlist_,int predecessor_,int successor_)
    {
        successorlist=(ArrayList)successorlist_.clone();
        predecessor=predecessor_;
        successor=successor_;
        
    }
    
    public int getSuccessor()
    {
        return successor;
    }
    
    public int getPredecessor()
    {
        return predecessor;
    }
    public ArrayList getSuccessorList()
    {
        return successorlist;
    }
}


class IndexEntry
{
    private byte[] hashkey;
    private int nodeid;
    double load;
    double timestamp;
    public IndexEntry(double load_,byte []hashkey_,int nodeid_)
    {        
        hashkey=hashkey_.clone();
        nodeid=nodeid_;
        load=load_;
    }
    
    public void getHashkey(byte []hashkey_)
    {
        int i;
        for(i=0;i<hashkey.length;i++)
        {
            hashkey_[i]=hashkey[i];
        }
    }
    
    public int getId()
    {
        return nodeid;
    }  
    
    public double getLoad()
    {
        return load;
    }
    
    public boolean isMatch(byte []hashkey_)
    {
        if(HashCode.compare(hashkey,hashkey_)==0)
            return true;
        else
            return false;
    }
    
    public void setTimestamp(double timestamp_)
    {
        timestamp=timestamp_;
    }
    
    public double getTimestamp()
    {
        return timestamp;
    }
}

class FeedbackRequest
{
 private int resource;
 private int successorid;
 private Object feedbackindexentry;
 
 public FeedbackRequest(int resource_,int successorid_,Object feedbackindexentry_)
 {
     resource=resource_;
     successorid=successorid_;
     feedbackindexentry=feedbackindexentry_;
 }
 
 public int getResource()
 {
     return resource;
 }
 
 public int getSuccessorId()
 {
     return successorid;
 }
  
 public Object getFeedbackIndexEntry()
 {
     return feedbackindexentry;
 }
  
}

/*
class FeedbackIndexEntry
{
    
    private byte[] previoushashkey;
    private byte[] currenthashkey;
    private int nodeid;
    private int previousindexnodeid;
    public FeedbackIndexEntry(byte []previoushashkey_,byte[]currenthashkey_,int nodeid_,int previousindexnodeid_)
    {
        previoushashkey=previoushashkey_.clone();
        currenthashkey=currenthashkey_.clone();
        nodeid=nodeid_;
        previousindexnodeid=previousindexnodeid_;
    }
    
    public void getPreviousHashkey(byte []hashkey_)
    {
        int i;
        for(i=0;i<previoushashkey.length;i++)
        {
            hashkey_[i]=previoushashkey[i];
        }
    }
    
    public void getCurrentHashkey(byte []hashkey_)
    {
        int i;
        for(i=0;i<currenthashkey.length;i++)
        {
            hashkey_[i]=currenthashkey[i];
        }
    }
    
    public int getId()
    {
        return nodeid;
    }  
   public int getpreviousIndexNodeId()
     {
         return previousindexnodeid;
     }
    
}

*/

class RangeQuery{
 private byte start[];
 
 private byte end[];
 RangeQuery(byte []start_,byte[] end_)
 {
     int comp;
     comp=HashCode.compare(start_,end_);
     if(comp<=0)
     {
        start=start_.clone();
        end=end_.clone();
     }
     else
     {
         start=end_.clone();
         end=start_.clone();
     }
 }
 
 public byte[]getStart()
 {
     return start;
 }
 
 public byte[]getEnd()
 {
     return end;
 }
 
 public boolean contains(byte[]key)
 {     
     if(HashCode.compare(start,key)<=0 && HashCode.compare(key,end)<=0)
         return true;
     else
         return false;
 }
    
}


class ClockPulse
{
    private int pulsecount;
    ClockPulse(int pulsecount_)
    {
        pulsecount=pulsecount_;
    }
    public int  getPulseCount()
    {
        return pulsecount;
    }
    
    public void setPulseCount(int pulsecount_)
    {
        pulsecount=pulsecount_;
    }
    
    public void increasePulseCount()
    {
        pulsecount++;
    }
}

class UtilizationFeed
{
    private double utilization;
    private int size;
    private int underutilized;//no of underutilized resources
    private int free;//no of free reources
    private int overutilized;//no of overutilized resources
    UtilizationFeed(ArrayList index)
    {
        Iterator i;
        IndexEntry indexentry;
        double currentload;
        
        free=0;
        underutilized=0;        
        overutilized=0;
        utilization=0.0;
        
        i=index.iterator();
        
        while(i.hasNext())
        {
            indexentry=(IndexEntry)i.next();
            currentload=indexentry.getLoad();
            utilization+=currentload;
            //folloowing if statements check for the status of current resource
            if(currentload>=0 && currentload<=.67)
            {
                free+=1;
                    
            }
            
            if(currentload>.67 && currentload<=.86)
            {
                underutilized+=1;
                    
            }
            
            if(currentload>.86 && currentload<=.99)
            {
                overutilized+=1;
                    
            }
        }
        size=index.size();
    }
    
    public double getUtilization()
    {
        return utilization;
    }
    
    public int getSize()
    {
        return size;
    }
    
    public int getFree()
    {
        return free;
    }
    
    public int getUnderUtilized()
    {
        return underutilized;
    }
    
    public int getOverUtilized()
    {
        return overutilized;
    }
}

/*
 *
 *Class to maintain group of nodes
 *
 *
 */
class Group
{
    int leader;
    ArrayList member;
    int maxelement;
    int count;
    public Group(int leader_)
    {
        leader=leader_;
        count=0;
        maxelement=40;
        member=new ArrayList();
    }    
    
    public void addMember(int nodeid_)
    {
        member.add(nodeid_);
        count++;
    }
    
    public boolean isFull()
    {
        if(count==maxelement)
        {
            return true;
        }
        return false;
    }
    
    public int getLeader()
    {
        leader=(Integer)member.get(member.size()-1);
        
        return leader;
    }
    
    public ArrayList getMember()
    {
        return member;
    }
}

class MemberEntry
{
    int nodeid;
    double load;
    public MemberEntry(int nodeid_,double load_)
    {
        nodeid=nodeid_;
        load=load_;
    }
    
    public int getNodeId()
    {
        return nodeid;
    }
    public double getLoad()
    {
        return load;
    }
    
     public void setNodeId(int nodeid_)
    {
        nodeid=nodeid_;
    }
    public void setLoad(double load_)
    {
        load=load_;
    }    
}

class GridJob
{
    long size;
    long time;
    GridJob(long time_,long size_)
    {
        size=size_;
        time=time_;
    }
    
    public long getSize()
    {
        return size;
    }
    
    public long getTime()
    {
        return time;
    }
}
/*
class Token
{
 private int free;
 private int underutilized;
 private int overutilized;
 public Token()
 {
  free=1;
  underutilized=1;
  overutilized=1;
 } 
 
 public void setValue(int free_,int underutilized_,int overutilized_)
 {
     free=free_;
     underutilized=underutilized_;
     overutilized=overutilized_;
 }
 
 public int getFree()
 {
     return free;
 }
 
 public int getUnderUtilized()
 {
     return underutilized;
 }
 
 public int getOverUtilized()
 {
     return overutilized;
 }
 
}
 **/
