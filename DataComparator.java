/*
 * DataComparator.java
 *
 * Created on January 1, 2003, 11:44 PM
 *
 * @author Siddharth Rai
 */

package gridmonitor;

import java.util.*;

class IndexEntryComparator implements Comparator
{
  public int compare(Object obj1, Object obj2)
  {
    IndexEntry entry1;
    IndexEntry entry2;

    byte hashkey1[] = new byte[20];
    byte hashkey2[] = new byte[20];

    entry1  = (IndexEntry)obj1;
    entry2  = (IndexEntry)obj2;

    entry1.getHashkey(hashkey1);
    entry2.getHashkey(hashkey2);         

    return (HashCode.compare(hashkey1, hashkey2) != 0) ? HashCode.compare(hashkey1, hashkey2) : 
      Double.compare(entry2.getTimestamp(), entry1.getTimestamp());
  }
}
 
class IndexedValueComparator implements Comparator
{
  public int compare(Object obj1, Object obj2)
  {
    IndexEntry entry1;
    IndexEntry entry2;

    entry1 = (IndexEntry)obj1;
    entry2 = (IndexEntry)obj2;

    return ((entry1.getLoad() == entry2.getLoad()) ? 
        Double.compare(entry2.getTimestamp(), entry1.getTimestamp()) :
        ((entry1.getLoad()<entry2.getLoad()) ? -1 : 1));
  }
}
 
class DoubleComparator implements Comparator
{
     public int compare(Object obj1, Object obj2)
     {
         double load1;
         double load2;

         load1 = ((MemberEntry)obj1).getLoad();
         load2 = ((MemberEntry)obj2).getLoad();

         return ((int)(Math.round(load1 * 100) - Math.round(load2 * 100)));
     }
}

 /*
 class FeedbackIndexEntryComparator implements Comparator
 {
     
     public int compare(Object obj1,Object obj2)
     {
         FeedbackIndexEntry entry1,entry2;
         byte hashkey1[]=new byte[20];
         byte hashkey2[]=new byte[20];
         entry1=(FeedbackIndexEntry)obj1;
         entry2=(FeedbackIndexEntry)obj2;
         entry1.getPreviousHashkey(hashkey1);
         entry2.getPreviousHashkey(hashkey2);
         
         return HashCode.compare(hashkey1,hashkey2);
     }
 }
  */
