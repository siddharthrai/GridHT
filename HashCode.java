/*
 * HashCode.java
 *
 * Created on March 20, 2008, 2:54 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package gridmonitor;

/**
 *
 * @author Administrator
 */
import java.security.*;
import java.util.*;
public class HashCode {
    
    public static void compute(int id,byte[]hash)
    {
        byte[]temp=new byte[20];
        int i;
        try{
        String id_str=Integer.toString(id);
        MessageDigest sha=MessageDigest.getInstance("SHA-1");
        sha.update(id_str.getBytes());
        temp=sha.digest();
        for(i=0;i<temp.length;hash[i]=temp[i],i++);
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
     }
     public static void compute(double id,byte[]hash)
    {
        byte[]temp=new byte[20];
        int i;
        try{
        String id_str=Double.toString(id);
        MessageDigest sha=MessageDigest.getInstance("SHA-1");
        sha.update(id_str.getBytes());
        temp=sha.digest();
        for(i=0;i<temp.length;hash[i]=temp[i],i++);
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
     }

     
     public static void computeConsistentHash(double id,byte[]hash)
     {
         byte hashkeyindex[][]=new byte[100][];
         int i;
         KeyComparator comp=new KeyComparator();
         for(i=0;i<100;i++)
         {
             hashkeyindex[i]=new byte[20];
             HashCode.compute(i,hashkeyindex[i]);
             
         }
         Arrays.sort(hashkeyindex,comp);
         for(i=0;i<hash.length;i++)
         {             
             hash[i]=hashkeyindex[(int)Math.round(id*100)][i];
             
         }        
         
        /* for(i=0;i<11;i++)
         {
             System.out.println(i+":"+HashCode.getString(hashkeyindex[i]));
         }
         for(i=1;i<10;i++)
         {
             System.out.print((1-((double)1/i))+": "+Math.round((1-((double)1/i))*10)+" ");
         }
         */ 
     }
     
     
     
     public static void decrement(byte[]hash)
    {
         int index=hash.length-1;
         while(hash[index]==0)
         {
             hash[index]=(byte)0xff;
             index--;
         }
                 
         hash[index]=(byte)(hash[index]-0x01);
    }
     
     public static void incrememt(byte[]hash)
    {
         int index=hash.length-1;
         
         while(hash[index]==0xff)
         {
             hash[index]+=1;
             index--;
         }
         hash[index]+=1;  
         
    }
    
     public static String getString(byte[]hashkey)
     {
         int i;
         String byte_str_byte,byte_str,hash_str=null;
         
         for(i=0;i<hashkey.length;i++)
      {
          byte_str=Integer.toHexString(hashkey[i]);  
          //System.out.println(hash2[0]);
          
          byte_str_byte=(byte_str.length()>=2)?byte_str.substring(byte_str.length()-2):"0"+byte_str;
          
          hash_str=(hash_str!=null)?hash_str+byte_str_byte:byte_str_byte;
      }
         //hashstring=hash_str;
         //System.out.println(hashstring);
         return hash_str;
     }
     
     
     public static int compare(byte []hash1,byte[]hash2)
     {
      String byte_str,hash1_str=null,hash2_str=null,byte_str_byte;
      int i;
      hash1_str=getString(hash1);
      hash2_str=getString(hash2);
      if(hash1_str.length()==hash2_str.length())
        return hash1_str.compareTo(hash2_str);
      else
          return hash1_str.length()-hash2_str.length();
     }
    
}


class KeyComparator implements Comparator
     {
         public int compare(Object obj1,Object obj2)
         {
             byte key1[],key2[];
             key1=(byte[])obj1;
             key2=(byte[])obj2;            
             return HashCode.compare(key1,key2);             
         }
     }