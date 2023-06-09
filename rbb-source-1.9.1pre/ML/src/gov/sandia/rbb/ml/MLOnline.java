 /*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.EventCache;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.RBBML.MLPart;
import gov.sandia.rbb.util.StringsWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * A problem definition specifies a type of "problem", which is a co-occurrence
 * of events that match a pattern (an array of tagsets)
 *
 * The problem definition monitors Events as they are created and detects
 * specified co-occurrences which are instantiated as Problem Instances.
 *
 * The Events whose co-occurrence constitutes a problem instance are called
 * 'individuals' because each unique combination constitutes a different problem set.
 * A problem definition may also involve 'groups' of events.  A group is a set
 * of Events matching a tagset, but they are treated collectively as a single
 * feature in an Observation (albeit an array, but with only one name) and
 * the number of Events in a group can vary over time during a single problem
 * instance, and can even be 0.
 *
 * The inputs defining the problem may have cross-references ("join")
 *
 * For example, say we want to watch for
 * situations where an aircraft is low on fuel relative to its distance from the
 * carrier.  A problem instance consists of the position of a plane, and the
 * fuel load of that same plane.  Assuming each entity is uniquely identified
 * by a callsign, this problem is specified with the inputTags:
 * side=friendly,domain=air,variable=position callsign,variable=fuel
 * findConcurrentEvents resolves the missing 'callsign' value by binding it
 * to whatever value is found in the callsign tag of the tagset to the left.
 * 
 * The triggers for ProblemEvaluator are:
 * init: due to creation of an Individual Event, a new combination of Individuals matching
 *   a MLOnline now exists, so a new ProblemInstance is created.
 *   (TODO: this should also occur if inputTags are added to an Event so it now permutationIsSubset)
 * observe: obs was added to any obs table associated with an Event.
 *   In most cases this means a row was added to a Timeseries table.
 * done: ProblemEvaluator.done() is called when:
 *   1. an end time is set for any of the contributing Events
 *   2. any of the related events is deleted (TODO: not implemented)
 *
 * Rationale for obs structures implementing this class:
 * The most common operation is expected to be eventDataAdded, which is triggered
 * whenever an observation is added to any of the timeSeries in the problem.
 * So, lookup from an Event ID to all matching problem instances should be fast.
 *
 * EventID -> ProblemInstances
 * 
 * Another frequent operation is checking whether a problem instance already
 * exists.  For that we keep a set of lists of EventIDs.
 * 
 * EventIDs
 * 
 * When an Event ends, all the associated ProblemInstances must be done(),
 * and all the other events referencing the probleminstance must be unlinked.
 * 
 * @author rgabbot
 */
public class MLOnline
{
    /**
     * The feature extractor for each problem instance is cloned from this.
     */
    private MLFeatureExtractor extractorFactory;

    /**
     * map each event ID to the problem instances to which it belongs.
     */
    private Map<Long, Set<ProblemInstance>> problemInstances;

    /**
     * map each set of EventIDs that, together, constitute a problem instance.
     * This is used to check whether a problem instance has already been created
     * for a given set of Events.
     */
    private Set<Set<Long>> problemEventIDs;


    ArrayList<Input> inputs = new ArrayList<Input>();
    ArrayList<Group> groups = new ArrayList<Group>();

    private Double timeDesynchronization;

    RBBML rbbml;

    /**
     * Create the problem definition.
     *<p>
     * timeDesynchronization: if obs for a problem may arrive out of order,
     * problem instances may not be noticed.  If not null, the timeDesynchronization
     * parameter specifies the the maximum amount in the past that new events or updates
     * can arrive and without problem instances being missed.  That is, if an event
     * is started at time T or obs is added at time T or the end time of an event is set
     * to time T, problem instances may be missed if obs is subsequent added at
     * a time before T-timeDesynchronization.
     *<p>
     * Threading: This instance will keep a reference to the rbb, and synchronize on
     * the Connection (rbb.db()) before making calls on it.  So if you (the caller)
     * are keeping the rbb to use for other purposes, you must also synchronize
     * on the Connection.  Alternately, open a separate Connection for other uses.
     *
     */
    public MLOnline(RBBML rbbml,
            String[] inputNames, RBBFilter[] inputFilters,
            String[] groupNames, RBBFilter[] groupFilters,
            MLFeatureExtractor extractorFactory,
            Double timeDesynchronization) throws Exception {
        this.timeDesynchronization = timeDesynchronization;
        this.extractorFactory = extractorFactory;
        this.rbbml = rbbml;

        RBB inputRBB = rbbml.getRBB(MLPart.SESSION);


        for(int i = 0; i < inputNames.length; ++i)
            inputs.add(new Input(inputNames[i], inputFilters[i]));

        for(int i = 0; i < groupNames.length; ++i)
            groups.add(new Group(inputRBB, groupNames[i], groupFilters[i]));

        this.problemInstances = new HashMap<Long, Set<ProblemInstance>>();
        this.problemEventIDs = new HashSet<Set<Long>>();

     }

    /**
     * For each input (whether an individual or a group) we cache the most
     * recent Samples to quickly generate Observations.
     */
    private class Input extends EventCache {
        String name;

        Input(String name, RBBFilter filter) throws Exception{
            super(rbbml.getRBB(MLPart.SESSION));
            super.setMaxSamples(2); // just keep last 2 values for extrapolation
            super.initCache(filter);
            this.name = name;
        }
        @Override
        public void eventAdded(RBB rbb, RBBEventChange.Added ec) {
            super.eventAdded(rbb, ec);
            try {
                checkForNewProblemInstances(ec.event.getStart());
            } catch(Exception e) {
                System.err.println("MLOnline.checkForNewProblemInstances threw exception: "+e.toString());
            }
        }

        @Override
        public void eventRemoved(RBB rbb, RBBEventChange.Removed ec)
        {
            super.eventRemoved(rbb, ec);

            // Keep in mind, this doesn'start0 mean an event is over, it means it's
            // deleted from the historical record.
            // This could have far reaching effects, because anything derived from
            // that event, such as derived features, would have to be removed
            // explicitly (if that is desired).

            try {
                // it is not clear evt.getEnd() is correct for this.
                closeoutEvent(ec.event, ec.event.getEnd());
            } catch (Exception e) {
                System.err.println("MLOnline.closeoutEvent threw exception: "+e.toString());
            }
        }

        @Override
        public void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ec)
        {
            // System.err.println("MLOnline.Input.eventDataAdded to event "+evt);
            super.eventDataAdded(rbb, ec);

            Set<ProblemInstance> probs = problemInstances.get(ec.event.getID());
            if(probs == null) {
                // System.err.println("this event isn't part of any problems, so return now to avoid any further work.");
                return;
            }

            if(!ec.schemaName.equals(H2STimeseries.schemaName)) {
                System.err.println("Warning: MLProblemDefinition.eventDataAdded called for non-Timeseries data on Event "+ec.event.toString()+".  Ignoring...");
                return;
            }

            final double time = H2STimeseries.getTimeFromRow(ec.data);

            // System.err.println("got new observation at t="+time);

            // observe problem instances only when the first input receives new data.
            for(ProblemInstance prob : probs) {
                // System.err.println("Checking problem headed by "+prob.observationMetadata.getAllFeatureEvents()[0].getID()+" to see if it matches Event "+evt.getID());
                if(!ec.event.getID().equals(prob.observationMetadata.getAllFeatureEvents()[0].getID()))
                    continue;
                try {
                    observeProblem(prob, time);
                } catch(Exception e) {
                    System.err.println("Exception thrown by MLOnline.observeProblem: "+e.toString());
                }
            }
        }

        @Override
        public void eventModified(RBB rbb, RBBEventChange.Modified ec)
        {
            try {
            // if the end has been set for this object, we take it to be over.
            // so all the associated problem instances are done() too.
            if(ec.event.getEnd() < gov.sandia.rbb.impl.h2.statics.H2SRBB.maxDouble())
                closeoutEvent(ec.event, ec.event.getEnd());
            }
            catch(java.sql.SQLException e) {
                System.out.println("Error updating Problem Instances during modification of event " + ec.event.toString());
            }
        }

        /*
         * TimeseriesCache has protected access to filter but MLOnline needs to access it.
         */
        RBBFilter getFilter() { return filter; };

    } // end of class Input

     private class Group extends EventCache {
        String name;
        MLObservation.Metadata metadata = null;
        
        Group(RBB rbb, String name, RBBFilter filter) throws SQLException{
            super(rbb);
            super.setMaxSamples(2); // just keep last 2 samples for extrapolating
            initCache(filter);
            this.name = name;
        }

        public MLObservation.GroupFeature observe(double time) throws SQLException {
            updateMetadata(time);
            MLObservation.GroupFeature groupFeature = new MLObservation.GroupFeature(time, metadata);
            for(Event e : metadata.getAllFeatureEvents()) {
                Timeseries ts = (Timeseries) e;
                final String groupMemberName = ts.getID().toString();
                // System.err.println("Group Member "+ts.getTagset()+" has value "+StringsWriter.join(",", ts.valueLinear(time, null))+" at time "+time);
                groupFeature.setFeature(groupMemberName, ts.valueLinear(time));
            }
            return groupFeature;
        }

        /**
         * this is a no-op unless this.metadata is first set to null.
         */
        private void updateMetadata(double time) throws SQLException {
            if(metadata != null)
                return;
            metadata = new Metadata();
            for(Timeseries ts : findTimeseries(super.filter, byTime(time,time))) {
                final String groupMemberName = ts.getID().toString();
                if(ts.getNumSamples()>0) // if it doesn't have a value it's still useless!
                    metadata.add(groupMemberName, ts);
            }
         }

        // It might seem that eventCreated would be of interest, but it's not,
        // because the group member is invalid until a value is observed for it.
        // @Override
        // public void eventCreated(RBB rbb, Event evt) {
        // }

        @Override
        public void eventRemoved(RBB rbb, RBBEventChange.Removed ec) {
            super.eventRemoved(rbb, ec);
            metadata = null; // one less group member now so we need to rebuild metadata next time.
        }

        @Override
        public void eventModified(RBB rbb, RBBEventChange.Modified ec) {
            super.eventModified(rbb, ec);
            // this MIGHT cause the set of group members in the next observation to change,
            // for example if the end time of a timeseries is set.
            metadata = null; 
        }

        @Override
        public synchronized void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ec) {
            try {
                super.eventDataAdded(rbb, ec);
                Timeseries[] ts = getTimeseriesByID(ec.event.getID());
                if(ts.length != 1)
                    return; // this really should not happen.
                // System.err.println("Data added to a group member");
                if (ts[0].getNumSamples() == 1 && metadata != null) { // if we just added the first observation of this it is now useful.
                    metadata.add(ec.event.getID().toString(), ts[0]);
                    // System.err.println("Now metadata has members "+metadata.getFeatureNames());
                } else {
                    // if it is null it will be re-calculated before being used again which is fine.
                }
            } catch (SQLException ex) {
                System.err.println("MLOnline.Group.eventDataAdded exception: "+ex.toString());
            }

        }


    } // end of class Group

    private void printProblemEventIDs(String msg)
    {
        System.err.print(msg);
        for(Set<Long> ids : this.problemEventIDs)
        {
            System.err.print("\t");
            for(Long id : ids)
            {
                System.err.print(" " + id);
            }
        }
        System.err.println();
    }

    private void checkForNewProblemInstances(double time) throws Exception
    {

        // The reason for this open time interval [time, null] - i.e. now until infinity -
        // is because obs arrives at the RBB asynchronously (may not arrive in time order).
        // So, it is possible Events
        // have already arrived in the future, so they won'start0 be discovered again unless we
        // discover them now.  But in this case they must start in the future (the later
        // of the two start times), and no ProblemEvaluator.observe() calls should be made
        // for obs timestamped before the start of the joint event.  And if this new event
        // ends before the other begins, we'll have created a spurious event.
        // To avoid matching this with events that happened

        // Part of the problem here is the RBB convention of starting an Event as open
        // ended by defaulting its end time to infinity, then ending it early when
        // it is discovered when it ended.  That causes overlaps with all future events
        // to happen, then "unhappen."

        // Imagine Event A starts at 7 and (initially) ends at infinity
        // Now Event B starts at 10
        // Now the end time for A is set to 8.
        // So in the end, there was no problem instance.
        // Unless two Events start at the same precise instant, there's no
        // way to know if they'll end up overlapping until at least one ends.
        // That's a nonstarter for 2 long-running Events that should be observed() frequently over time.
        // Say we assume the end time, if infinity, is at least the last observation added to it.

        // Tentative problem instance.

        // A "Confirmed End" is the latest of the Start Time, End Time excluding infinity, or Event Data for that Event.
        // A Problem is Tentative until the Earliest Confirmed End is at or after the latest Start Time of all events in the Problem.
        // Only then can init be called.

        // final double maxDesynchronization = 10.0;

        RBB inputRBB = rbbml.getRBB(MLPart.SESSION);

        synchronized(inputRBB.db()) {

        // get tagsets for all inputs in a single array
        RBBFilter[] inputFilters = new RBBFilter[inputs.size()];
        for(int i = 0; i < inputs.size(); ++i)
            inputFilters[i] = inputs.get(i).getFilter();

        // find all current problem instances.
        ResultSet rs = H2SEvent.findConcurrent(
            inputRBB.db(), inputFilters, null, time, time, null, null);

        int numProblems = 0;
        int numNewProblems = 0;

            while (rs.next())
            {
                ++numProblems;

                Long[] eventIDs = (Long[]) rs.getArray("IDS").getArray();
                Set<Long> eventIDSet = new HashSet<Long>();

                // copy array into set.
                for (int i = 0; i < eventIDs.length; ++i)
                    eventIDSet.add(eventIDs[i]);

                if (this.problemEventIDs.contains(eventIDSet))
                    continue; // already had this one.

                // yay, found a new problem instance.
                ++numNewProblems;

    //        Metadata fn = new Metadata();
    //        fn.add("x", null);
    //        fe.addSelfToFeatureNames(fn);

                try {
    //                Event[] events = H2SEvent.getCopiesByID(rbb.db(), eventIDs);

                    // create the problem instance.

                    ProblemInstance prob = new ProblemInstance(
                        constructMetadata(eventIDs),
                        extractorFactory.clone());

                    prob.extractor.setRBBML(rbbml);
                    prob.extractor.init(time, prob.observationMetadata);

                    // update this.problemInstances
                    for (int i = 0; i < eventIDs.length; ++i)
                    {
                        final Long eventID = eventIDs[i];
                        Set<ProblemInstance> problems = problemInstances.get(eventID);
                        // if this is the first problem for this  Event, we'll have to make a new problem set.
                        if (problems == null)
                        {
                            problems = new HashSet<ProblemInstance>();
                            problemInstances.put(eventID, problems);
                        }
                        problems.add(prob);
                    }

                    // update problemEventIDs
                    this.problemEventIDs.add(eventIDSet);
                }
                catch (Exception exception)
                {
                    // do nothing.  If this problem instance can'start0 be created,
                    // don'start0 store it, and don'start0 interfere with other problem instances.
                    System.err.println("caught exception while creating new problem instance in checkForNewProblemInstances: "
                        + exception.toString());
                }
            }
        }
        // System.err.println("checkForNewProblemInstances found " + numProblems + " problems, " + numNewProblems + " of which were new");
    }

    private Metadata constructMetadata(Long[] timeseriesIDs) throws SQLException {
        Metadata md = new MLObservation.Metadata();
        for(int i = 0; i < inputs.size(); ++i)
            md.add(inputs.get(i).name, inputs.get(i).getEventsByID(timeseriesIDs[i])[0]);
        for(int i = 0; i < groups.size(); ++i)
            md.add(groups.get(i).name, null);
        extractorFactory.addSelfToFeatureNames(md);
        return md;
    }

    private synchronized void closeoutEvent(Event evt, Double time) throws SQLException
    {
        if(problemInstances.get(evt.getID())==null)
            return; // this event wasn't in any problem instances anyways, so no change.

          for(ProblemInstance problem : problemInstances.get(evt.getID())) {
            // the end of ANY input to a problem ends it,
            // and it is ended for ALL the inputs that went into it:
            for(Long id : problem.getEventIDs()) {
                if(id.equals(evt.getID()))
                    continue; // removing from the problemInstancesSet we're iterating over is not allowed (concurrent modification exception).  Do it after the loop.
                if(!problemInstances.get(id).remove(problem))
                    System.err.println("Warning: MLProblemDefinition.closeoutEvent: problemEventIDs was out of sync with problemInstances..."+StringsWriter.join(",", problem.getEventIDs().toArray())+" not found");
                if(problemInstances.get(id).isEmpty())
                    problemInstances.remove(id);
            }
            if(!problemEventIDs.remove(problem.getEventIDs()))
                System.err.println("Warning: MLProblemDefinition.closeoutEvent: problemEventIDs was out of sync with problemInstances..."+StringsWriter.join(",", problem.getEventIDs().toArray())+" not found");
    
            try {
                problem.extractor.setRBBML(rbbml);
                problem.extractor.done(time);
            }
            catch(Exception e) {
                System.err.println("allProblemsForEventDone caught exception thrown from "+problem.getClass().getName()+".done(): "+e.toString());
            }
        }
        problemInstances.remove(evt.getID());

        // System.err.println("Now problemEventIDs.size()="+problemEventIDs.size()+" and problemInstances.size()="+problemInstances.size());
    }

    private void observeProblem(ProblemInstance prob, Double time) throws Exception {
        MLObservation obs = new MLObservation(time, prob.observationMetadata);
        for(Input input : inputs) {
            Timeseries ts = (Timeseries) prob.observationMetadata.getFeatureEvent(input.name);
            if(ts.getNumSamples()==0)
                return; // cannot create an observation if any of the inputs are empty.
            Float[] interpolatedValue = ts.valueLinear(time);
            // System.err.println("Inteprolated value of timeseries "+ts.getID()+" is "+StringsWriter.join(",",interpolatedValue));
            obs.setFeature(input.name, interpolatedValue);
        }
        for(Group group : groups)
            obs.setFeature(group.name, group.observe(time));

        prob.extractor.setRBBML(rbbml);
        prob.extractor.observe(obs);
    }


    static class ProblemInstance
    {
        MLObservation.Metadata observationMetadata;

        // the extractor for this problem instance.
        MLFeatureExtractor extractor;

        ProblemInstance(MLObservation.Metadata observationMetadata, MLFeatureExtractor extractor) throws SQLException
        {
            this.extractor = extractor;
            this.observationMetadata = observationMetadata;
        }

        Set<Long> getEventIDs() {
            Set<Long> result = new HashSet<Long>();
            for(Event e : observationMetadata.getAllFeatureEvents())
                result.add(e.getID());
            return result;
        }
    }

}
