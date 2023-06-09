
package gov.sandia.rbb.ui;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventListener;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.Timer;

/**
 *
 * RBBEventChangeUI addresses two needs for Swing programs that update in response
 * to changes in RBB Events:
 *<p>
 * First, it dispatches notifications in the Swing event-dispatching thread, so
 *   it is threadsafe to do GUI-related processing in the callback.
 *   (RBB EventListeners, on the other hand, are called in a different thread).
 *<p>
 * Second, it allows the GUI to update in a lazy fashion.
 *   If one RBB Event is modified, then
 *   it is likely that others will soon be modified as well.  So waiting
 *   until things settle down before redrawing reduces churn.  This is controlled
 *   by the laziness and impatience parameters to the constructor.
 *<p>
 *  To use this, you must override eventChageUI, which will be called in the
 *  Swing thread, and add the RBBEventChange UI as a listener (by calling addEventListener) on
 *  an RBB, or on an EventCache.
 *
 * @author rgabbot
 */
public abstract class RBBEventUI extends RBBEventListener.Accumulator {
    /**
     *
     * @param rbb: rbb in which event changes are of interest.  This same instance will be provided in the notifications.
     * @param tags: tags of events of interest
     * @param swingListener: will be called in the Swing Event Thread with Event changes.
     * @param laziness: milliseconds to wait after one event change to see if additional events change before firing the action.
     * @param impatience: to prevent starvation, stop being lazy after this many milliseconds, even if event changes continue to occur steadily.  May be null (infinite).
     * @throws Exception
     */
    public RBBEventUI(int laziness, Integer impatience) throws Exception {
        this.impatience = impatience;
        timer = new Timer(laziness, new ActionListener() {
            @Override public void actionPerformed(ActionEvent e) {
                rbbEventUI(getEventChanges());
            }});
        timer.setRepeats(false);
    }

    public abstract void rbbEventUI(RBBEventChange[] changes);

    @Override public synchronized void eventChanged(RBB rbb, RBBEventChange eventChange) {
        super.eventChanged(rbb, eventChange);
        
        if(impatience != null && timer.isRunning()) {
            // if the timer is already running, then a redraw is already pending.
            boolean tiredOfWaiting = (System.currentTimeMillis()-timeFired) > impatience;
            if(tiredOfWaiting) {
                // don't cancel the pending timer event... go ahead and let it fire, because we have grown impatient.
            }
            else {
                // be paitent and restart the wait for subsequent event changes.
                timer.restart();
            }
        }
        else {
            // if the timer was not running, start it.
            timer.restart();
        }

    }

    private Timer timer;
    private long timeFired=0;
    private Integer impatience;
}
