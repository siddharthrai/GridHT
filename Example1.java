/*
 * Example1.java
 *
 * Created on March 19, 2008, 4:06 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package gridmonitor;

/**
 *
 * @author Administrator
 */
import eduni.simjava.Sim_system;
import java.security.*;
import java.util.*;
import java.io.*;
public class Example1 {
    
    /** Creates a new instance of Example1 */
    public static void main(String [] args) {
        int i;
        String line;
        String parts[];
        GridMonitorResource res[]=new GridMonitorResource[500];
        GridMonitorBroker broker[]=new GridMonitorBroker[500];
        RandomAccessFile util,job;
        long time,size;
        try{
         util=new RandomAccessFile("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_utilization.dat","rw");
         //job=new RandomAccessFile("./inputdata/das2_arrival_abs.dat","rw");
         //line=job.readLine();
         
        // parts=line.split(" ");
         //System.out.println(parts[0]+":"+parts[1]);
      /*
        try {
            int i=10;
            //Random rand=new Random();
            //rand.()
            ArrayList a1,a2;
            a1=new ArrayList();
            a1.add((Integer)0x11);
            a1.add((Integer)0x12);
            a1.add((Integer)0x13);
            a2=(ArrayList)a1.clone();
            a1=null;
            byte test[]=new byte[20];
            for(i=0;i<20;test[i]=(byte)0x00,i++);
                String hash_byte=null;
            String str;
            byte b=(byte)0xff;
            String idstr=String.valueOf(i);
            MessageDigest sha=MessageDigest.getInstance("SHA-1");
            
            sha.update((byte)'a');
            byte hashbyte[];
            hashbyte=sha.digest();
            String hash=hashbyte.toString();
            //System.out.println(hashbyte.length);
            //System.out.println();
            for(i=0;i<20;i++) {
                str=Integer.toHexString(test[i]);
                str=(test[i]!=0)?str.substring(str.length()-2):"00";
                hash_byte=(hash_byte!=null)?hash_byte+str:str;
                }
            System.out.println((Integer)a2.get(2));
        } catch(Exception e){
            System.out.println(e.getMessage());}
            
      */
        /*byte hash[][]=new byte[3][20];
        
        
        HashCode.computeConsistentHash((double)0.0,hash[0]);
        System.out.println(" 0.0 "+HashCode.getString(hash[0]));
        
         HashCode.computeConsistentHash((double)0.75,hash[1]);
        System.out.println(" 0.75 "+HashCode.getString(hash[1]));
        
         HashCode.computeConsistentHash((double)0.91,hash[2]);         
        System.out.println(" 0.91 "+HashCode.getString(hash[2]));
        RangeQuery query=new RangeQuery(hash[0],hash[2]);
        System.out.println("**** "+query.contains(hash[1]));
        //HashMap map=new HashMap();
        //int a=1,b=1;
        //map.put(a,10);
        //System.out.println("*****"+map.containsKey(b));
        
       */ 
        //System.out.println("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_schedule_trace.dat");
       /*try
       {
           RandomAccessFile in=new RandomAccessFile("./inputdata/das2_fs0.dat","r");
           for(i=0;i<10;i++)
               System.out.println(Double.parseDouble(in.readLine()));
       }catch(IOException e){}
        */
       
         
        
        
       
        
            GridMonitor gm=new GridMonitor();
            
            //String str1=new String("ac3478d69a3c81fa62e6f5c3696165a4e5e6ac5");
            // String str2=new String("356a192b7913b04c54574d18c28d46e6395428ab");            
            //String str3=new String("bd307a3ec329e1a2cff8fb87480823da114f8f4");
            //System.out.println(new String("b").compareTo(new String("311")));
            
            for(i=0;i<160;i++)
            {
                res[i]=new GridMonitorResource("res"+i,gm.getAdminId(),util);
            }
            for(i=0;i<1;i++)
            {
                broker[i]=new GridMonitorBroker("broker"+i,gm.getAdminId(),"das2_fs"+i+".dat");
            }
            
            
            
            gm.startsimulation(); 
            
            util.close();
           
        }catch(Exception e){
            
        }
    

        
    }
    
}
