/*
 * GridMonitorTags.java
 *
 * Created on January 1, 2003, 3:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package gridmonitor;

/**
 *
 * @author Administrator
 */
public class GridMonitorTags 
{
public static final int BASE=0;
public static final int ADMIN=100;
public static final int DHT=200;
public static final int RESOURCE=300;
public static final int CLOCK_PULSE=1000;
public static final String file="periodicupdate";
////////////////////////////////////////////////////
public static final double SCHEDULE_IMM = 0.0;
public static final double SCHEDULE_NOW = 10.0;
public static final int RETRY=-1;
public static final int PING=4;
public static final int PONG=5;
////////////////////////////////////////////////////
public static final int ROLE_GET=ADMIN+1;
public static final int ROLE_INDEX_DHT_NODE=ADMIN+2;
public static final int ROLE_FEEDBACK_DHT_NODE=ADMIN+3;
public static final int ROLE_FEEDBACK_ADMIN=ADMIN+4;
public static final int ROLE_RESOURCE_LEADER=ADMIN+5;
public static final int ROLE_RESOURCE=ADMIN+6;
public static final int ADD_BROKER=ADMIN+7;
public static final int BROKER_ADDED=ADMIN+8;
public static final int JOIN_COMPLETE=ADMIN+9;
public static final int GROUP=ADMIN+10;
public static final int START=ADMIN+11;
////////////////////////////////////////////////////

public static final int KEY_LOOKUP=DHT+1;
public static final int FIND_SUCCESSOR=DHT+2;
public static final int SUCCESSOR=DHT+3;
public static final int DHT_JOIN=DHT+4;
public static final int JOINED=DHT+5;
public static final int SET_PREDECESSOR=DHT+6;
public static final int SET_SUCCESSOR=DHT+7;
public static final int INDEX=DHT+8;
public static final int START_DHT_INDEXING=DHT+9;
public static final int KEY_RESOURCE=DHT+10;
public static final int MORE_RESOURCE=DHT+11;
public static final int KEY_NOT_FOUND=DHT+12;
public static final int PRINT=DHT+13;
public static final int GET_A_INDEX_NODE=DHT+14;
public static final int A_INDEX_NODE=DHT+15;
public static final int GET_INDEX_SIZE=DHT+16;
public static final int COUNT=DHT+17;
public static final int UPDATE_INDEX=DHT+18;
public static final int REMOVE=DHT+19;
public static final int REMOVED=DHT+20;
public static final int INDEX_UPDATED=DHT+21;

public static final int GET_A_FEEDBACK_NODE=DHT+22;
public static final int A_FEEDBACK_NODE=DHT+23;
public static final int UPDATE_FEEDBACK=DHT+24;
public static final int BATCH_INDEX=DHT+25;
public static final int BATCH_INDEXED=DHT+26;
public static final int REMOVE_FEEDBACK=DHT+27;

public static final int REMOVE_INDEX_ENTRY=DHT+28;
////////////////////////////////////////////////////////
public static final int GET_LOAD=RESOURCE+1;
public static final int LOAD=RESOURCE+2;
public static final int VERIFY=RESOURCE+3;
public static final int VERIFY_ACCEPT=RESOURCE+4;
public static final int VERIFY_REJECT=RESOURCE+5;
public static final int INDEX_FEEDBACK=RESOURCE+6;
public static final int SET_INDEXNODE=RESOURCE+7;
public static final int UTILIZATION_FEED=RESOURCE+8;
public static final int SET_LEADER=RESOURCE+9;
public static final int SET_LOAD=RESOURCE+10;
public static final int SEND_FEEDBACK=RESOURCE+11;
public static final int SEND_ACK=RESOURCE+12;
///////////////////////////////////////////////////////
}
