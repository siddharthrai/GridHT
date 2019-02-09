/*
 * GridMonitorDHTStub.java
 *
 * Created on January 2, 2003, 12:54 AM
 *
 * @author Siddharth Rai
 *
 */

package gridmonitor;

import java.util.*;
import gridsim.*;
import eduni.simjava.*;

public class GridMonitorDHTStub
{
  /** Creates a new instance of GridMonitorDHTStub */
  GridMonitorResource resource;

  ArrayList successorlist;
  ArrayList fingertable;
  ArrayList index;

  int predecessor;
  int successor;
  int indexreq;
  int update_req;
  int query_fail;
  int query_pass;
  
  public GridMonitorDHTStub(GridMonitorResource resource_) 
  {
    successorlist = new ArrayList();
    fingertable   = new ArrayList();

    index       = new ArrayList();
    resource    = resource_;
    indexreq    = 0;
    update_req  = 0;
    query_fail  = 0;
    query_pass  = 0;
  }

  /*
   * Finds successor node for a given node-id 
   *
   * Arguments:
   *  node-id to be matched
   *
   *  Return:
   *    Current DHT node id, if it is successor else nearest finger id
   */

  public int get_successor(byte[] hashkey) 
  {
    int nodeid;

    if(containsid(hashkey) >= 0) 
    {            
      return resource.nodeid;
    }

    return this.successor;
  }

  /*
   * Checks if node-id is in successorlist
   *
   * Arguments:
   *  arg :id to be matched
   *
   * Returns
   *  true for successor list, false otherwise
   *
   */

  public int containsid(byte[] hashkey) 
  {
    SuccessorList temp;

    Iterator i = successorlist.iterator();

    int count = -1; //stores part of successor list that contains id(0-1)  

    while (i.hasNext()) 
    {
      byte start[]  = new byte[20];
      byte end[]    = new byte[20];

      count++;

      temp = (SuccessorList)i.next();
      temp.getStart(start);

      temp.getEnd(end);          

      if (HashCode.compare(start, hashkey) <= 0 && HashCode.compare(hashkey, end) <= 0)
      {
        //System.out.println(HashCode.getString(hashkey) + " contained in range " + 
        //    HashCode.getString(start) + " " + HashCode.getString(end) + 
        //    " at node " + resource.nodeid);
        //return count;
        return 1;
      }
    }

    return -1;
  }

  /*
   * Gets nearest fingerentry
   *
   * Arguments
   *  node-id : input node id
   *
   * Returns
   *  Nearest finger
   */

  private int getnearest(byte[] hashkey) 
  {
    int i;
    int mdiff;
    int id;

    mdiff = 0x7fff;
    id    = 0;

    FingerEntry finger;

    for(i = 0; i < fingertable.size(); i++) 
    {
      finger = (FingerEntry)fingertable.get(i);
      if (Math.abs(HashCode.compare(finger.getHashKey(), hashkey)) < mdiff) 
      {
        id    = finger.getId();
        mdiff = Math.abs(HashCode.compare(finger.getHashKey(), hashkey));
      }
    }

    return id;
  }

  /*
   * Adds new key to index
   *
   */

  public void addToIndex(Object entry)
  { 
    boolean found;
    int node_id;
    IndexEntry next_entry;
    
    byte hashkey[] = new byte[20];
    ((IndexEntry)entry).setTimestamp(Sim_system.clock());

    Iterator    i;
    
    i = index.iterator();

    found = false;
    
    while(i.hasNext())
    {
      next_entry=(IndexEntry)i.next();
      ((IndexEntry)next_entry).getHashkey(hashkey);
      node_id = ((IndexEntry)next_entry).getId();
      
      if(((IndexEntry)entry).isMatch(hashkey) == true && node_id == ((IndexEntry)entry).getId())
      {
        found = true;
        break;
        //return destinationid;
        //System.out.println("Found in index at " + resource.nodeid);
      }
    }
    
    if (found == false)
    {
      index.add(0, entry);
    
      update_req++;
    }
    
    if(index.size() > 20)
    {
      //System.out.println("***Index exceeded size at "+this.resource);
    }

     //System.out.println("Added to index at " + resource.nodeid);
  }

  /*
   * Initializes the first DHT node
   *
   */

  public void initFirstNode(byte[]hashkey) 
  {
    int  i;
    byte start[]  = new byte[20];
    byte end[]    = new byte[20];
    byte hash[]   = new byte[20];

    hash=hashkey.clone();

    for(i = 0; i < 20; start[i] = (byte)0x00, i++);

    SuccessorList temp=new SuccessorList(start,hash);        
    successorlist.add(temp);

    for(i=0; i < 20; end[i] = (byte)0xff, i++);

    HashCode.incrememt(hash);

    temp = new SuccessorList(hash, end);

    successorlist.add(temp);

    successor   = resource.nodeid;
    predecessor = resource.nodeid;
  }

  /*
   * Checks for the key in index
   *
   * Arguments:
   *  hashkey : hashkey to be checked
   *
   * Returns 
   *  destination-id if key is present, -1 otherwise
   *
   */

  public ArrayList isInIndex(byte[]hashkey)
  {
    Iterator    i;
    IndexEntry  entry;

    ArrayList destinationid = new ArrayList();

    i = index.iterator();

    while(i.hasNext())
    {
      entry=(IndexEntry)i.next();
      if(entry.isMatch(hashkey) == true)
      {
        destinationid.add(entry.getId());
        this.removeFromIndex(entry);
        //return destinationid;
        //System.out.println("Found in index at " + resource.nodeid);
      }
    }

    return destinationid;
  }

  /*
   *
   *
   */
  public int removeFromIndex(IndexEntry entry_)
  {
    Iterator    i;
    IndexEntry  entry;
    IndexEntry  temp;

    int pos=-1;
    int count=0;

    byte indexkey[] = new byte[20];

    i = index.iterator();

    entry_.getHashkey(indexkey);

    while (i.hasNext())
    {
      entry = (IndexEntry)i.next();

      count++;

      if (entry.getId() == entry_.getId())
      {
        //destinationid=entry.getId();
        //entry.

        if (pos > -1)
        {
          temp = (IndexEntry)index.get(pos);
          if (Double.compare(temp.getTimestamp(), entry.getTimestamp()) > 0)
          {
            pos = count - 1;
          }
        }
        else
        {
          pos = count - 1;
        }

        //index.remove(entry);
        //System.out.println("entry with key "+HashCode.getString(indexkey)+" removed...");
        //return 0;
      }
    }

    if (pos != -1)
    {
      temp = (IndexEntry)index.get(pos);
      //System.out.println("Entry of node "+temp.getId()+" has been removed value is "+temp.getLoad()+" at "+Sim_system.clock());
    }
    else
    {
      System.out.println("******************************value cant be removed*****************************"+entry_.getId()+" "+this.resource.nodeid+" at "+Sim_system.clock());
    }

    index.remove(pos); 

    //System.out.println("entry cant be removed at "+this.resource.nodeid+" send parameters are "+HashCode.getString(indexkey) +" "+entry_.getId());

    /*System.out.println("Current index on is:");
      i=index.iterator();

      while(i.hasNext())
      {
      entry=(IndexEntry)i.next();
      entry.getHashkey(indexkey);
      System.out.println(entry.getId()+" "+HashCode.getString(indexkey));
      }
      */
    return 0;
  }

  public ArrayList getIndex(RangeQuery query)
  {
    Iterator i = index.iterator();

    IndexEntry  entry;
    ArrayList   destinationid = new ArrayList();

    byte hashkey[] = new byte[20];
    int  state=0;

    indexreq++;
    
    System.out.println("Current Index size " + index.size());
    
    while (i.hasNext())
    {
      entry = (IndexEntry)i.next();
      entry.getHashkey(hashkey);

      //System.out.println(HashCode.getString(hashkey));
      //System.out.println(entry.getId()+" "+entry.getLoad()+" "+entry.getTimestamp()+" ");

      if (query.contains(hashkey) == true)
      {
        destinationid.add(entry);
      }
    }
    
    if (destinationid.size() != 0)
    {
      query_pass++;
    }
    else
    {
      query_fail++;
    }
    
    System.out.println("Found in index at node " + resource.nodeid + " update =  " + 
        update_req + " query = " + indexreq + " query pass = " + query_pass + 
        " query fail = " + query_fail);
    
    //System.out.println("Found in index at node " + resource.nodeid + " entries =  " + destinationid.size() + " " + 
    //    HashCode.getString(query.getStart()) + " " + HashCode.getString(query.getEnd()));
    //System.out.println("Index size "+destinationid.size()+" "+HashCode.getString(query.getStart())+" "+HashCode.getString(query.getEnd()));
    return destinationid;
  }


  public ArrayList getUpdate(RangeQuery query)
  {
    Iterator i = index.iterator();

    IndexEntry entry;
    ArrayList destinationid=new ArrayList();

    byte hashkey[] = new byte[20];        

    IndexEntryComparator comp = new IndexEntryComparator(); 

    //System.out.println("\n----------------------------");
    while (i.hasNext())
    {
      entry = (IndexEntry)i.next();

      entry.getHashkey(hashkey);

      //System.out.println(HashCode.getString(hashkey));
      if (query.contains(hashkey) == true)
      {
        destinationid.add(entry);
        i.remove();
      }
    }

    Collections.sort(destinationid,comp);

    i = destinationid.iterator();

    //System.out.println("Index of size "+destinationid.size()+" send by "+this.resource);
    while (i.hasNext())
    {
      entry = (IndexEntry)i.next();
      entry.getHashkey(hashkey);
      //System.out.println("######"+entry.getLoad()+"######"+entry.getId());
    }

    //System.out.println("Index size "+destinationid.size()+" "+HashCode.getString(query.getStart())+" "+HashCode.getString(query.getEnd())+" at node "+this.resource.nodeid);
    // System.out.println("\n-----------------------------");
    return destinationid;
  }
}