/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb;

import java.util.ArrayList;

/**
 *
 * @author rgabbot
 */
public interface RBBEventListener {

    /**
    * EventListener is implemented by clients that will receive a callback when Events matching
    * some tagset are changed.  To receive a callback implement this interface and
    * call RBB.addEventListener()
    */
    void eventAdded(RBB rbb, RBBEventChange.Added ec);

    void eventModified(RBB rbb, RBBEventChange.Modified ec);

    void eventRemoved(RBB rbb, RBBEventChange.Removed ec);

    void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ec);



    /**
     * Here is a trivial implementation that routes event changes of any type to 'eventChanged',
     * which also does nothing by default.
     * <p>
     * To ignore types of event changes you are not interested in, you can override
     * the corresponding RBBEventListener method to do nothing.
     */
    public static class Adapter implements RBBEventListener {
        public void eventChanged(RBB rbb, RBBEventChange eventChange) { }
        @Override public void eventAdded(RBB rbb, RBBEventChange.Added ec) { eventChanged(rbb,ec); }
        @Override public void eventModified(RBB rbb, RBBEventChange.Modified ec) { eventChanged(rbb,ec); }
        @Override public void eventRemoved(RBB rbb, RBBEventChange.Removed ec) { eventChanged(rbb,ec); }
        @Override public void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ec) { eventChanged(rbb,ec); }
    }

    /**
     *
     * Accumulate event changes in a buffer indefinitely until they are retrieved with getEventChanges()
     * This class is very useful to handle RBBEventListener notification in a thread-safe way.
     * <p>
     * To perform event-driven processing, you can override eventChanged
     * (which RBB calls from a notification thread) to schedule
     * the desired thread to call getEventChanges() and dispatch the results
     * (just be sure to call super.eventChanged in your override so the change is actually stored!)
     * For example see gov.sandia.rbb.ui.RBBEventChangeUI
     * <p>
     * To ignore types of event changes you are not interested in, you can override
     * the corresponding RBBEventListener method to do nothing.
     *
     */
    public static class Accumulator extends Adapter {
        /**
         * Retrieves all the changes received since the last invocation; empty array if none.
         */
        public synchronized RBBEventChange[] getEventChanges() {
            RBBEventChange[] currentChanges = null;
            if(changes == null) {
                currentChanges = new RBBEventChange[0];
            }
            else {
                currentChanges = changes.toArray(new RBBEventChange[0]);
                changes = null;
            }
            return currentChanges;
        }

        @Override public synchronized void eventChanged(RBB rbb, RBBEventChange eventChange) {
            if(changes == null)
                changes = new ArrayList<RBBEventChange>();
            changes.add(eventChange);
        }

        private ArrayList<RBBEventChange> changes;
    }

}
