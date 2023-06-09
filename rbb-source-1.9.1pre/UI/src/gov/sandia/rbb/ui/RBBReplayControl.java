    
package gov.sandia.rbb.ui;

import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventListener;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.SQLException;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * synchronization using an RBB Timeseries.
 *<p>
 * The tagset of the ts event is type=timeControl
 *<p>
 * The ts has one observation.  Its columns are:
 * <br>
 * TIME: system time.  (stored in TIME because it's a double, and system time is generally the milliseconds since the UNIX Epoch, i.e. a big number)
 * <br>
 * C1: sim time
 * <br>
 * C2: replay rate
 * <br>
 * C3: a random number that is the SenderID, used to break feedback loops.
 *
 *<p>
 * This class allows multiple replayers to stay in sync so long as their system clocks
 * are synchronized (e.g. using ntp).  The key is that the current sim time is
 * computed on demand as the time since replay was commanded times the replay speed.
 *
 * @author rgabbot
 */
public class RBBReplayControl extends RBBEventListener.Adapter {

    /*
     * The listener is notified through this call whenever the current time changes,
     * either because somebody set the plan state in the RBB, or
     * on the animation timer.
     */
    public interface Listener {
        public void replayControl(long simTime, double playRate);
    }

    /**
     * The previous system time at which a change in replay state was ordered (e.g. clicking Play)
     */
    private Long systemTime;

    /*
     * The simTime when the previous change in replay state was ordered.
     */
    private Long simTime;

    /*
     * The current replay rate as a multiple of realtime.  0 = paused.
     */
    private Double playRate;

    /**
     * 
     * @param newSystemTime: may be (and normally will be) null, if so it is set to the current system time.
     * @param newSimTime: may be null, if so it is set to getSimTime()
     * @param newPlayRate: may be null, if so it is left at what it was before.
     * @param updateRBB: If called by application code this is normally specified as true, which means to
     *                   send the new replay state to other listeners of the RBB and NOT notify the listener
     *                   of this ReplayControl instance (normally the caller) (the RBB update notification is
     *                   ignored when receive in response to update by self).
     * @throws SQLException
     */
    public synchronized void set(Long newSystemTime, Long newSimTime, Double newPlayRate, boolean updateRBB) throws SQLException {

        if(newSimTime == null)
            simTime = getSimTime(); // if getSimTime() is to be called, it must be before altering any variables.
        else
            simTime = newSimTime;

        if(newSystemTime == null)
            systemTime = System.currentTimeMillis();
        else
            systemTime = newSystemTime;

        if(newPlayRate != null)
            playRate = newPlayRate;

        if(timer != null) {
            if(playRate == null || playRate <= 0.0)
                timer.stop();
            else
                timer.start();
        }

        // if the change originated locally, update the RBB;
        // otherwise it originated remotely so notify the listener.
        if(updateRBB)
            updateRBB();
        else
            SwingUtilities.invokeLater(new Runnable() {
                @Override public void run() {
                    notifyListener();
                }
            });
    }

    /**
     * This is called internally by the code and not needed by users of the class.
     * This is called from the Swing update thread (only!)
     *
     */
    public synchronized void notifyListener() {
        if(listener != null)
            listener.replayControl(getSimTime(), getPlayRate());
    }

    public synchronized Double getPlayRate() {
        refreshIfNeeded();
        return playRate;

    };

    /*
     * This is a non-op if this instance is event-driven.
     * But if this was initialized without a listener, calls the RBB
     * to get the latest replay state, but supresses any exception raised.
     */
    private void refreshIfNeeded() {
        if (rbb == null)
            return;

        if(listener != null)
            return; // this instance gets updates pushed to it.

        try {
            updateFromRBB();
        } catch (SQLException ex) {
            System.err.println("RBBReplayControl.refreshIfNeeded Error: "+ex);
        }
    }


    /**
     * Compute the current sim time based on previously set parameters.
     * Returns null if this has not been initialized.     *
     */
    public synchronized Long getSimTime() {
        refreshIfNeeded();

        if(this.systemTime == null ||
                this.playRate == null ||
                this.simTime == null)
            return null;

       return (long)((System.currentTimeMillis() - this.systemTime)*this.playRate + this.simTime);
    }

    public boolean isPlaying() {
        refreshIfNeeded();
        return playRate > 0;
    }

    /**
     * Set the play rate as of right getSimTime.
     *
     * @param playRate
     */
    public void setPlayRate(double playRate) throws SQLException
    {
        set(null, null, playRate, true);
    }

    public void setSimTime(long simTime) throws SQLException {
        set(null, simTime, null, true);
    }

    private RBB rbb;

    /**
     * The ReplayControl timeseries is never deleted, so its ID should not
     * change.  Thus we store it here to avoid finding it all the time.
     */
    private Timeseries timeseries;
    
    /**
     * the uuid is stored as a float because it will be stored as a column
     * in a ts, which is float sample.  Any excess precision would be lost,
     * resulting in not recognizing self.
     */
    private final float uuid = java.util.UUID.randomUUID().hashCode();

    /**
     * This tagset must uniquely identify the timeseries that will be used
     * to synchronize the RBB Replay Control Transport.
     *<p>
     * By changing the tags, it should be possible to have multiple sets of
     * synchronized times in the same RBB
     *
     */
    public final static String timeTags = "type=timeControl";

    private Listener listener;

    /**
     * This is used to implement animation.
     */
    private Timer timer;

    /**
     * Connect this transport to the specified RBB.
     *<p>
     * The listener will be notified when the replay state is modified, or
     * the current play rate is nonzero and animationMS milliseconds have passed.
     * Call disconnect() to disable this transport before the end of the program,
     * Until disconnect() is called, the process will keeping listening and responding
     * to updates indefinitely.  (The RBB must be in another process for this to work).
     *<p>
     * Note, this actually connects the transport to the database underlying the RBB instance;
     * the java RBB object instance can go away, or be disconnected from the underlying database, etc.
     * yet the transport will still work.
     *
     */
    public RBBReplayControl(RBB rbb, Listener listener, Integer animationMs) throws SQLException {
        this(rbb);

        this.listener = listener;

        if(listener != null) {
            setAnimation(animationMs);
            rbb.addEventListener(this, byTags(timeTags));
        }
    }

    /*
     * This is the non-event-driven constructor.
     * No listener will be notified, and each call to get... will result in
     * a database query.
     */
    public RBBReplayControl(RBB rbb) throws SQLException {
        this.rbb = rbb;
        updateFromRBB();
    }

    /**
     * dissassociate from the RBB.
     *
     * we do not rbb.disconnect() because we never did rbb.connect() - the rbb is just handed to us.
     */
    public synchronized void disconnect()
    {
        try {
            rbb.removeEventListener(this);
        } catch(SQLException e) {
        
        }
        rbb = null;
        timeseries = null;
    }

    public synchronized void setAnimation(Integer ms) {
        if(ms != null && timer != null && timer.getDelay() == ms)
            return; // no-op

        if(ms == null) {
            if(timer != null)
                timer.stop();
        }
        else {
            if(timer != null)
                timer.setDelay(ms);
            else {
                timer = new Timer(ms.intValue(),
                    new ActionListener() {
                        @Override public void actionPerformed(ActionEvent e) {
                            notifyListener();
                        }
                    });
            }

        if(isPlaying())
            timer.start();
        }
    }


    /**
     * Users of this class don't call this.
     * It is called by the RBB as a notification that somebody set the replay state,
     * by a dedicated notification thread created by the RBB
     */
    @Override
    public synchronized void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ec) {
        try {
            Float[] sample = H2STimeseries.getSampleFromRow(ec.data);
            if(sample[2] == (float) uuid)
                return; // don't respond to changes sent by this instance

            long newSystemTime = H2STimeseries.getTimeFromRow(ec.data).longValue();
            long newSimTime = (long)Float.parseFloat(sample[0].toString());
            double newPlayRate = Double.parseDouble(sample[1].toString());

            set(newSystemTime, newSimTime, newPlayRate, false);

            // System.err.println("RBBReplayControlTransport.eventDataAdded " + this.ts.getID() + " to " + replayState.toString());
        } catch(Exception e) {
            System.err.println("RBBReplayControlTransport.eventDataAdded Exception: "+e.toString());
        }
    }

    private void updateRBB() throws SQLException {
        // System.err.println("RBBReplayControl updating RBB");
        H2SEvent.setStartByID(rbb.db(), timeseries.getID(), (double) systemTime); // this discards any previous setting that is now out of date
        H2STimeseries.addSampleByID(rbb.db(), timeseries.getID(),
            (double)systemTime,
            new Float[]{ simTime.floatValue(), playRate.floatValue(), uuid },
            null, null);

    }

    /*
     * Pull the current replay state from the RBB.
     * If the current replay state in the RBB is invalid, initialize it.
     */
    private void updateFromRBB() throws SQLException {
        Timeseries[] ts = Timeseries.findWithSamples(rbb.db(), byTags(timeTags));

        // ensure there is exactly 1 timeControl ts, and it has exactly 1 sample.
        // todo: yes, this is a race condition.  Need an atomic rbb stored procedure for this.

        if (ts.length == 1) // there should be exactly 1 timeseries event.
            timeseries = ts[0];
        else
        {
            if(ts.length > 1)
                H2SEvent.delete(rbb.db(), byTags(timeTags));
            // do this part whether the problem was too many or too few synchronization events before.
            timeseries = new Timeseries(rbb, 3, 0.0, new Tagset(timeTags));
        }

        if(timeseries.getNumSamples() == 1) {
            Timeseries.Sample s = timeseries.getSample(0);
            systemTime = s.getTime().longValue();
            simTime = s.getValue()[0].longValue();
            playRate = s.getValue()[1].doubleValue();
        }
        else {
            systemTime = 0L;
            simTime = 0L;
            playRate = 0.0;
            updateRBB();
        }
    }
}
