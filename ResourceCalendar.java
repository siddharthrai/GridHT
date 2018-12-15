/*
 * Title:        GridSim Toolkit
 * Description:  GridSim (Grid Simulation) Toolkit for Modeling and Simulation
 *               of Parallel and Distributed Systems such as Clusters and Grids
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * $Id: ResourceCalendar.java,v 1.16 2005/04/01 06:30:36 anthony Exp $
 */


package gridmonitor;

import java.util.*;
import eduni.simjava.Sim_system;
import gridsim.*;
/**
 * GridSim ResourceCalendar class implements a mechanism to support modeling
 * a local load on Grid resources that may vary according to the time zone,
 * time, weekends and holidays.
 *
 * @author       Manzur Murshed and Rajkumar Buyya
 * @since        GridSim Toolkit 1.0
 * @invariant $none
 */
public class ResourceCalendar
{
    private Random random_;
    private double timeZone_;
    private double[] weekdayLoad_;  // load during the day
    private double[] holidayLoad_;  // load during holidays, including weekends
    private LinkedList weekendList_;
    private LinkedList holidayList_;
    private final int TIME = 60;


    // LinkedList weekend is a list of 0 = Sunday, 1 = Monday, 2 = Tuesday,
    // 3 = Wednesday, 4 = Thursday, 5 = Friday, 6 = Saturday
    /**
     * Allocates a new ResourceCalendar object
     * @param timeZone      time zone
     * @param peakLoad      the load during peak time, with range: [0 ... 1]
     * @param offPeakLoad   the load during off peak time, with range: [0 ... 1]
     * @param relativeHolidayLoad   the load during holidays, 
     *                              with range: [0 ... 1]
     * @param weekendList   a list of Integer numbers for weekends
     * @param holidayList   a list of Integer numbers for holidays
     * @param seed          the initial seed
     * @pre timeZone >= 0.0
     * @pre seed > 0
     * @post $none
     */
    public ResourceCalendar(double timeZone, double peakLoad,
                double offPeakLoad, double relativeHolidayLoad,
                LinkedList weekendList, LinkedList holidayList, long seed)
    {
        random_ = new Random(seed);
        this.timeZone_ = timeZone;

        // load must be within [0 .. 1] range
        int FULL = 1;
        if (peakLoad > FULL) {
            peakLoad = FULL;
        }

        if (offPeakLoad > FULL) {
            offPeakLoad = FULL;
        }

        if (relativeHolidayLoad > FULL) {
            relativeHolidayLoad = FULL;
        }

        // initialised as per common observation of relative local
        // usage behavior of resource per hour in a day
        // NOTE: This is similar to background load of a resource
        double[] regularLoad = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,   // 0 - 6 am
                                 0.0, 0.1, 0.2, 0.4, 0.8, 1.0,   // 7 - 11 am
                                 1.0, 0.6, 0.6, 0.9, 1.0, 1.0,   // 12 - 5 pm 
                                 0.5, 0.2, 0.1, 0.0, 0.0, 0.0 }; // 6 - 11.59pm 

        double val = 0.0;
        double FACTOR = 0.1;
        int HOUR = 24;
        weekdayLoad_ = new double[HOUR];  // background load during weekdays
        holidayLoad_ = new double[HOUR];  // background load during holidays

        // for each hour in a day, determine the load during the weekday and
        // holiday
        for (int i = 0; i < HOUR; i++)
        {
            val = regularLoad[i] * (peakLoad - offPeakLoad) + offPeakLoad;

            // background load during the week
            weekdayLoad_[i] = GridSimRandom.real( val, FACTOR, FACTOR,
                                    random_.nextDouble() );

            // background load during the holiday
            holidayLoad_[i] = GridSimRandom.real( relativeHolidayLoad * val, 
                                    FACTOR, FACTOR, random_.nextDouble() );

            // if the load is full, it means that a resource can't process 
            // any jobs. Hence, need to lower the load
            if (weekdayLoad_[i] >= FULL) {
                weekdayLoad_[i] = 0.95;
            }

            if (holidayLoad_[i] >= FULL) {
                holidayLoad_[i] = 0.95;
            }
        }

        this.weekendList_ = weekendList;
        this.holidayList_ = holidayList;
       // System.out.println("calendar initialized....");
    }

    /**
     * Gets a Calendar object for a specified simulation time
     * @param simulationTime    the simulation time
     * @return a Calendar object
     * @deprecated As of GridSim 2.1, replaced by
     *             {@link #getCalendarAtSimulationTime(double)}
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre simulationTime >= 0.0
     * @post $result != null
     */
    public Calendar CalendarAtGivenSimTime(double simulationTime)
                throws NullPointerException
    {
        return this.getCalendarAtSimulationTime(simulationTime);
    }

    /**
     * Gets a Calendar object for a specified simulation time
     * @param simulationTime    the simulation time
     * @return a Calendar object
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre simulationTime >= 0.0
     * @post $result != null
     */
    public Calendar getCalendarAtSimulationTime(double simulationTime)
                throws NullPointerException
    {
        Calendar calendar = GridMonitor.getSimulationCalendar();
        Date date = GridMonitor.getSimulationStartDate();

        if (date == null)
        {
            throw new NullPointerException(
                    "ResourceCalendar.getCalendarAtSimulationTime() : Error - "+
                    "Need to call GridSim.init() first before using any of " +
                    "GridSim entities.");
        }

        // Set calendar time as the Simulation start date-time at 0:00:00 GMT
        calendar.setTime(date);

        // Adjust calendar time for time zone and simulation time
        calendar.add(Calendar.MINUTE, (int) (timeZone_ * TIME));
        calendar.add(Calendar.SECOND, (int) simulationTime);

        return calendar;
    }

    /**
     * Gets the current Calendar object (based on the simulation clock)
     * @return the current Calendar object
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @see eduni.simjava.Sim_system#clock()
     * @deprecated As of GridSim 2.1, replaced by
     *             {@link #getCurrentCalendar()}
     * @pre $none
     * @post $result != null
     */
    public Calendar CalendarNow() throws NullPointerException {
        return this.getCurrentCalendar();
    }

    /**
     * Gets the current Calendar object (based on the simulation clock)
     * @return the current Calendar object
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @see eduni.simjava.Sim_system#clock()
     * @pre $none
     * @post $result != null
     */
    public Calendar getCurrentCalendar() throws NullPointerException {
        return this.getCalendarAtSimulationTime( Sim_system.clock() );
    }

    /**
     * Gets the current simulation time
     * @param localTime     a Calendar object
     * @return the current simulation time
     * @deprecated As of GridSim 2.1, replaced by
     *             {@link #getSimulationTime(Calendar)}
     * @throws NullPointerException if localTime is <tt>null</tt>
     *              or <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre localTime != null
     * @post $result >= 0.0
     */
    public double SimulationTime(Calendar localTime)
                throws NullPointerException
    {
        return this.getSimulationTime(localTime);
    }

    /**
     * Gets the current simulation time
     * @param localTime     a Calendar object
     * @return the current simulation time
     * @throws NullPointerException if localTime is <tt>null</tt>
     *              or <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre localTime != null
     * @post $result >= 0.0
     */
    public double getSimulationTime(Calendar localTime)
                          throws NullPointerException
    {
        if (localTime == null) {
            throw new NullPointerException(
                    "ResourceCalendar.getSimulationTime() : Error - " +
                    "Calendar object must not be null.");
        }

        localTime.add( Calendar.MINUTE, (int) (timeZone_ * TIME) );

        Date date = GridSim.getSimulationStartDate();
        if (date == null)
        {
            throw new NullPointerException(
                    "ResourceCalendar.getSimulationTime() : Error - " +
                    "Need to call GridSim.init() first before using any of " +
                    "GridSim entities.");
        }

        Calendar start = getCalendarAtGivenDate(date);
        Date localDate = localTime.getTime();
        Date startDate = start.getTime();
        double time = (localDate.getTime() - startDate.getTime()) / 1000.0;

        return time;
    }

    /**
     * Gets a Calendar object at the specified date
     * @param date  the Date object
     * @return the Calendar object
     * @deprecated As of GridSim 2.1, replaced by
     *             {@link #getCalendarAtGivenDate(Date)}
     * @pre date != null
     * @post $result != null
     */
    public Calendar CalendarAtGivenDate(Date date) {
        return this.getCalendarAtGivenDate(date);
    }

    /**
     * Gets a Calendar object at the specified date
     * @param date  the Date object
     * @return the Calendar object
     * @pre date != null
     * @post $result != null
     */
    public Calendar getCalendarAtGivenDate(Date date)
    {
        Calendar calendar = GridSim.getSimulationCalendar();
        if (calendar == null) {
            calendar = Calendar.getInstance();
        }

        calendar.setTime(date);

        // Adjust calendar time for time zone
        calendar.add( Calendar.MINUTE, (int) (timeZone_ * TIME) );
        return calendar;
    }

    /**
     * Checks whether the current simulation time is a holiday or not
     * @return <tt>true</tt> if it is a holiday, otherwise returns
     *         <tt>false</tt>
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @deprecated As of GridSim 2.1, replaced by {@link #isHoliday()}
     * @pre $none
     * @post $result == true || $result == false
     */
    public boolean IsHoliday() throws NullPointerException {
        return this.isHoliday();
    }

    /**
     * Checks whether the current simulation time is a holiday or not
     * @return <tt>true</tt> if it is a holiday, otherwise returns
     *         <tt>false</tt>
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre $none
     * @post $result == true || $result == false
     */
    public boolean isHoliday() throws NullPointerException
    {
        Calendar myCalendar = getCurrentCalendar();
        int day_of_year = myCalendar.get(Calendar.DAY_OF_YEAR);
        int day_of_week = myCalendar.get(Calendar.DAY_OF_WEEK);

        if (holidayList_ != null)
        {
            if ( holidayList_.contains(new Integer(day_of_year)) ) {
                return true;
            }
        }

        if (weekendList_ != null)
        {
            if ( weekendList_.contains(new Integer(day_of_week)) ) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks whether the given date is a holiday or not
     * @param date  the Date object
     * @return <tt>true</tt> if it is a holiday, otherwise returns
     *         <tt>false</tt>
     * @deprecated As of GridSim 2.1, replaced by {@link #isHoliday(Date)}
     * @pre date != null
     * @post $result == true || $result == false
     */
    public boolean IsHoliday(Date date) {
        return this.isHoliday(date);
    }

    /**
     * Checks whether the given date is a holiday or not
     * @param date  the Date object
     * @return <tt>true</tt> if it is a holiday, otherwise returns
     *         <tt>false</tt>
     * @pre date != null
     * @post $result == true || $result == false
     */
    public boolean isHoliday(Date date)
    {
        if (holidayList_ == null) {
            return false;
        }

        Calendar myCalendar = getCalendarAtGivenDate(date);
        myCalendar.setTime(date);

        int day_of_year = myCalendar.get(Calendar.DAY_OF_YEAR);
        int day_of_week = myCalendar.get(Calendar.DAY_OF_WEEK);

        if (holidayList_ != null)
        {
            if ( holidayList_.contains(new Integer(day_of_year)) ) {
                return true;
            }
        }

        if (weekendList_ != null)
        {
            if (weekendList_.contains(new Integer(day_of_week)) ) {
                return true;
            }
        }

        return false;
    }

    /**
     * Gets the current load for the current simulation time
     * @return the current load
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @deprecated As of GridSim 2.1, replaced by {@link #getCurrentLoad()}
     * @pre $none
     * @post $result >= 0.0
     */
    public double CurrentLoad() throws NullPointerException {
        return this.getCurrentLoad();
    }

    /**
     * Gets the current load for the current simulation time
     * @return the current load
     * @throws NullPointerException if <tt>GridSim.init()</tt> has not been
     *              called before
     * @see gridsim.GridSim#init(int, Calendar, boolean, String[], String[],
     *          String)
     * @pre $none
     * @post $result >= 0.0
     */
    public double getCurrentLoad() throws NullPointerException
    {
        if ( isHoliday() ) {
            return holidayLoad_[getCurrentCalendar().HOUR_OF_DAY];
        }
        else {
            return weekdayLoad_[getCurrentCalendar().HOUR_OF_DAY];
        }
    }

} // end class

