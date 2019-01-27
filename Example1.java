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
import gridsim.*;
import eduni.simjava.*;
public class Example1 {
    
    /** Creates a new instance of Example1 */
  
  
    public static void main(String [] args) {
        int i;
        String line;
        String parts[];
        
        int query_count;
        double query_time;
        int job_count;
        long job_success;
        
        boolean with_gridlet;
        
        ArrayList clocked_nodes;

        
        clocked_nodes = new ArrayList();
               
        RandomAccessFile util, job;
        FileWriter query_count_trace;
        FileWriter query_time_trace;
        FileWriter job_count_trace;
        
        long time, size;
        
        int nodes;
        
        nodes = 128;
        
        query_count = 0;
        job_count   = 0;
        job_success = 0;
        
        with_gridlet = false;
        
        //for (int itr = 0; itr < 2; itr++)
        {
          ClockPulseGenerator global_clock;
                  
          GridMonitorResource res[]  = new GridMonitorResource[500];
          GridMonitorBroker broker[] = new GridMonitorBroker[500];
        
          clocked_nodes.clear();
          
          try
          {        
            
            Sim_system.initialise();
            
            //util=new RandomAccessFile("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_utilization.dat","rw");
         //job=new RandomAccessFile("./inputdata/das2_arrival_abs.dat","rw");
         //line=job.readLine();
         
         if (args.length > 1)
         {
           System.out.println(args[0]);
           String gl = new String("with_gridlet");
           
           if (gl.equals(args[0]) == true)
           {
             with_gridlet = true;
           }          
           
           nodes = Integer.valueOf(args[1]);
           
           System.out.println("Nodes set to " + nodes);
           
           query_count_trace = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_query_count" + "_" + args[0] + "_" + args[1] + ".csv", true);
           query_time_trace = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_query_time" + "_" + args[0] + "_" + args[1] + ".csv", true);
           job_count_trace = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_job_count" + "_" + args[0] + "_" + args[1] + ".csv", true);
         }  
         else
         {
           nodes = Integer.valueOf(args[0]);
         
           System.out.println("Nodes set to " + nodes);
           
           query_count_trace = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_query_count" + "_without_gridlet_" + args[0] + ".csv", true);
           
           query_time_trace = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_query_time" + "_without_gridlet_" + args[0] + ".csv", true);
           
           job_count_trace = new FileWriter("./trace/"+GridMonitorTags.file+"/"+GridMonitorTags.file+"_job_count" + "_without_gridlet_" + args[0] + ".csv", true);
         }
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
        
            GridMonitor gm = new GridMonitor();
            
            int admin = gm.getAdminId();
            clocked_nodes.add(admin);
            
            //GridMonitorAdmin new_admin1 = new GridMonitorAdmin("admin");
            //GridMonitorAdmin new_admin2 = new GridMonitorAdmin("admin");
            
            //int admin1 = new_admin1.getNodeId();
            //clocked_nodes.add(admin1);
            
            //int admin2 = new_admin2.getNodeId();
            //clocked_nodes.add(admin2);
            
            //new_admin1.setPeerNode(admin2);
            
            //String str1=new String("ac3478d69a3c81fa62e6f5c3696165a4e5e6ac5");
            // String str2=new String("356a192b7913b04c54574d18c28d46e6395428ab");            
            //String str3=new String("bd307a3ec329e1a2cff8fb87480823da114f8f4");
            //System.out.println(new String("b").compareTo(new String("311")));
            
            for (i = 0; i < nodes; i++)
            {
                res[i] = new GridMonitorResource("res" + i, gm.getAdminId(), null, clocked_nodes, with_gridlet);
                System.out.println("Finished instantiating res " + i);
                //clocked_nodes.add(res[i].getNodeId());
            }
            
            for (i = 0; i < 1; i++)
            {
                broker[i] = new GridMonitorBroker("broker" + i, gm.getAdminId(), nodes, "das2_fs" + i + ".dat", clocked_nodes, with_gridlet);
                //clocked_nodes.add(broker[i].getNodeId());
            }
            
            System.out.println("Total clocked nodes " + clocked_nodes.size());
            
            global_clock = new ClockPulseGenerator(0, 4, clocked_nodes);
            
            gm.setClockPulseGenerator(global_clock);
            
            broker[0].setClockPulseGenerator(global_clock);
            
            for (i = 0; i < nodes; i++)
            {
              res[i].setClockPulseGenerator(global_clock);
            }
                                   
          //  new_admin1.setClockPulseGenerator(global_clock);
          //  new_admin2.setClockPulseGenerator(global_clock);
            
            gm.startsimulation(); 
          
            Sim_system.run();
            
            query_count = broker[0].getQueryCount();
            query_time  = broker[0].getQueryTime();
            job_count   = broker[0].getJobCount();
            
            for (i = 0; i < nodes; i++)
            {
                job_success = job_success + res[i].getSubmitted();
            }
            
            gm.finish();            
            gm = null;
            
            for (i = 0; i < nodes; i++)
            {
              res[i] = null;
            }
            
            broker[0] = null;
        
            global_clock = null;
            //util.close();           
            
            query_count_trace.write("Queries " + query_count + "\n");
            query_time_trace.write("Query-Time " + query_time + "\n");
            job_count_trace.write("Jobs " + job_count + "\n");
            
            query_count_trace.close();
            query_time_trace.close();
            job_count_trace.close();
          }
          catch(Exception e)
          {   
            System.out.println("Exception in Example at " + e.getMessage());
            //System.out.println("Exception in Example at " + e.getStackTrace()[0].getLineNumber());
          }
        }
             
        
        
        System.out.println("Total query " + query_count + " job count " + job_count + " job success " + job_success);
    }    
}