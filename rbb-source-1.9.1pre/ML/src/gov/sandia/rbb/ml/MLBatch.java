 /*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.*;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.RBBML.MLPart;
import gov.sandia.rbb.util.StringsWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
//
///**
// *
// * A problem definition specifies a type of "problem", which is a co-occurrence
// * of events that match a pattern (an array of tagsets)
// *
// * The problem definition monitors Events as they are created and detects
// * specified co-occurrences which are instantiated as Problem Instances.
// *
// * The Events whose co-occurrence constitutes a problem instance are called
// * 'individuals' because each unique combination constitutes a different problem set.
// * A problem definition may also involve 'groups' of events.  A group is a set
// * of Events matching a tagset, but they are treated collectively as a single
// * feature in an Observation (albeit an array, but with only one name) and
// * the number of Events in a group can vary over time during a single problem
// * instance, and can even be 0.
// *
// * The inputs defining the problem may have cross-references ("join")
// *
// * For example, say we want to watch for
// * situations where an aircraft is low on fuel relative to its distance from the
// * carrier.  A problem instance consists of the position of a plane, and the
// * fuel load of that same plane.  Assuming each entity is uniquely identified
// * by a callsign, this problem is specified with the inputTags:
// * side=friendly,domain=air,variable=position callsign,variable=fuel
// * findConcurrentEvents resolves the missing 'callsign' value by binding it
// * to whatever value is found in the callsign tag of the tagset to the left.
// *
// * The triggers for ProblemEvaluator are:
// * init: due to creation of an Individual Event, a new combination of Individuals matching
// *   a MLProblemDefinition now exists, so a new ProblemInstance is created.
// *   (TODO: this should also occur if inputTags are added to an Event so it now permutationIsSubset)
// * observe: obs was added to any obs table associated with an Event.
// *   In most cases this means a row was added to a Timeseries table.
// * done: ProblemEvaluator.done() is called when:
// *   1. an end time is set for any of the contributing Events
// *   2. any of the related events is deleted (TODO: not implemented)
// *
// * Rationale for obs structures implementing this class:
// * The most common operation is expected to be eventDataAdded, which is triggered
// * whenever an observation is added to any of the timeSeries in the problem.
// * So, lookup from an Event ID to all matching problem instances should be fast.
// *
// * EventID -> ProblemInstances
// *
// * Another frequent operation is checking whether a problem instance already
// * exists.  For that we keep a set of lists of EventIDs.
// *
// * EventIDs
// *
// * When an Event ends, all the associated ProblemInstances must be done(),
// * and all the other events referencing the probleminstance must be unlinked.
// *
// * @author rgabbot
// */
//public class MLBatch
//{
//    public static ResultSet batch(
//        RBBML rbbml,
//        String[] inputNames,
//        Tagset[] inputTags,
//        String[] groupNames,
//        Tagset[] groupTags,
//        Object[] skip,
//        MLFeatureExtractor fe,
//        Double start,
//        Double end,
//        Double timestep,
//        Tagset timeCoordinate)
//        throws Exception
//    {
//        org.h2.tools.SimpleResultSet result = new org.h2.tools.SimpleResultSet();
//        result.addColumn("START_TIME", java.sql.Types.DOUBLE, 20, 0);
//        result.addColumn("END_TIME", java.sql.Types.DOUBLE, 20, 0);
//        result.addColumn("IDS", java.sql.Types.ARRAY, 20, 0);
//
//        RBB inputRBB = rbbml.getRBB(MLPart.SESSION);
//
//        if (inputRBB.db().getMetaData().getURL().equals("jdbc:columnlist:connection"))
//            return result;
//
//        ResultSet rs = H2SEvent.findConcurrent(inputRBB.db(), Tagset.multiToArray(inputTags), skip, start, end,
//                timeCoordinate==null ? null : timeCoordinate.toArray(), null);
//
//        while(rs.next())
//        try {
//            Double problemStartTime = rs.getDouble("START_TIME");
//            if(start != null && problemStartTime < start)
//                problemStartTime = start;
//
//            Double problemEndTime = rs.getDouble("END_TIME");
//            if(end != null && problemEndTime > end)
//                problemEndTime = end;
//
//            Long[] eventIDs = (Long[]) rs.getArray("IDS").getArray();
//
//            Double[] observationTimes = RBBML.makeObservationTimes(inputRBB, eventIDs[0], problemStartTime, problemEndTime, start, end,
//                    fe.getMaxWarmup(), fe.getMaxCooldown(), timestep, timeCoordinate, null);
//
//            batchInstance(rbbml, inputNames, groupNames, groupTags,
//                eventIDs, fe, problemStartTime, problemEndTime, observationTimes, timeCoordinate);
//
//            // the problem instance was executed with no exception, so report it to the caller.
//            result.addRow(problemStartTime, problemEndTime, rs.getObject("IDS"));
//
////        } catch(ProblemSpecificException e) {
////            // don't add to results, but keep executing problem instances.
////            System.err.println(e.getMessage());
//        } catch(SQLException e) {
//            if(e.getSQLState() != null && e.getSQLState().equals(H2STimeseries.RBB_EMPTY_TIMESERIES)) {
//                System.err.println("MLBatch: " + e.getMessage());
//                // this exception affects only this problem set and doesn't mean other problem sets will fail, so continue.
//            }
//            else {
//               throw e;
//            }
//        }
//        // could do rs.beforeFirst() and return to caller, but beforeFirst() doesn'start0 seem to do anything.
//        // rs.beforeFirst();
//
//        fe.batchDone();
//
//        return result;
//    }
//
//    /*
//     * A ProblemSpecificException is one that effects only this problem instance,
//     * not a more general problem like SQL Connection broken.
//     */
//    public static class ProblemSpecificException extends Exception {
//        ProblemSpecificException(Long[] eventIDs, String s) {
//            super("MLBatch.batchProblemInstance: skipping problem set ("+StringsWriter.join(",", eventIDs) + "): "+s);
//        }
//    }
//
//    /**
//     * Batch evaluate a single problem instance, specified by a set of eventIDs
//     *
//     * @param inputRBB
//     * @param outputRBB
//     * @param inputNames
//     * @param groupNames
//     * @param groupTags
//     * @param eventIDs
//     * @param fe
//     * @param problemStartTime
//     * @param problemEndTime
//     * @param timestep
//     * @throws Exception:  If the error is problem-specific, an instance of ProblemSpecificInstance will be thrown.
//     */
//    public static void batchInstance(
//        RBBML rbbml,
//        String[] inputNames,
//        String[] groupNames,
//        Tagset[] groupTags,
//        Long[] eventIDs,
//        MLFeatureExtractor fe,
//        double problemStartTime,
//        double problemEndTime,
//        Double [] observationTimes,
//        Tagset timeCoordinate) throws Exception {
//
//        RBB inputRBB = rbbml.getRBB(MLPart.SESSION);
//
//        // Make sure all the Events are timeseries.
//        //
//        // It's easy to break this rule using
//        // predict -storeEvents
//        // because the events inherit inputTags from the inputs, and can be therefore
//        // be mistakenly used as inputs for new problem instances.
//        //
//        // If this happens, you probably need to use
//        // predict -resultTags
//        // so results have inputTags to distinguish them from inputs.
//        for(Long eventID : eventIDs)
//            if(!H2STimeseries.isTimeSeries(inputRBB.db(), eventID))
//                throw new ProblemSpecificException(eventIDs, "Event "+eventID+ " is not a timeseries.");
//        if(eventIDs.length != inputNames.length)
//            throw new Exception("ProblemDefinition.batch - the number of input tagsets doesn't match the number of input names!");
//
//        ResultSet samples = H2STimeseries.resampleValues(inputRBB.db(), eventIDs, observationTimes, timeCoordinate==null?null:timeCoordinate.toArray());
//
//        MLObservation.Metadata md = constructMetadata(inputRBB, inputNames, groupNames, fe, eventIDs);
//
//        fe.setRBBML(rbbml);
//
//        // use the problem start time,
//        // which may be before observationTimes[0]
//        fe.init(problemStartTime, md);
//
//        // find all the members of each group input that will be observed sometime during this problem instance.
//        if(groupNames==null)
//            groupNames = new String[0];
//        Group[] groups = new Group[groupNames.length];
//        for(int iGroup = 0; iGroup < groupNames.length; ++iGroup)
//            groups[iGroup] = new Group(inputRBB, groupNames[iGroup], groupTags[iGroup], observationTimes, timeCoordinate);
//
//        for(int numObs=0; numObs < observationTimes.length; ++numObs)
//        {
//            samples.next();
//            final double t0 = observationTimes[numObs];
//            // build the current observation using the next interpolated value for each timeseries.
//            MLObservation obs = new MLObservation(t0, md);
//            for(int i = 0; i < inputNames.length; ++i)
//                obs.setFeature(inputNames[i], (Object[])samples.getArray(i+2).getArray());// +2 because first col is time and getArray(n) is 1-based
//
//            for(int iGroup = 0; iGroup < groups.length; ++iGroup)
//                obs.setFeature(groupNames[iGroup], groups[iGroup].observe(t0, numObs));
//
//            fe.observe(obs);
//        }
//
//        fe.done(problemEndTime);
//    }
//
//    static MLObservation.Metadata constructMetadata(RBB rbb,
//        String[] inputNames,
//        String[] groupNames,
//        MLFeatureExtractor fe, Long[] eventIDs) throws SQLException {
//        Metadata md = new MLObservation.Metadata();
//        Event[] events = H2SEvent.getCopiesByID(rbb.db(), eventIDs);
//        for(int i = 0; i < inputNames.length; ++i)
//            md.add(inputNames[i], events[i]);
//        for(int i = 0; groupNames != null && i < groupNames.length; ++i)
//            md.add(groupNames[i], null);
//        fe.addSelfToFeatureNames(md);
//        return md;
//    }
//
//    /**
//     * This is all the information we track about each group member during
//     * each problem instance.
//     */
//    private static class GroupMember {
//        Event event;
//        Float[][] samples;
//        String name;
//        GroupMember(Event e, int n) {
//            event=e;
//            name = e.getID().toString();
//            samples = new Float[n][];
//        }
//        String getName() {
//            return name;
//        }
//    }
//
//    private static class Group extends ArrayList<GroupMember> {
//        MLObservation.Metadata metadata;
//
//        Group(RBB rbb, String groupName, Tagset groupTags, Double[] times, Tagset timeCoordinate) throws SQLException {
//            // load up the initial set of members.  This will be updated if/when subsequent events are created.
//            for(Event memberEvent : Event.find(rbb.db(), groupTags, times[0], times[times.length-1], null, H2STimeseries.schemaName))
//                add(new GroupMember(memberEvent, times.length));
//            ResultSet rs = H2STimeseries.resampleValues(rbb.db(), getMemberIDs(), times, timeCoordinate==null?null:timeCoordinate.toArray());
//            for(int i = 0; i < times.length; ++i) {
//                rs.next();
//                for(int iMember=0; iMember<size(); ++iMember) {
//                    java.sql.Array a = rs.getArray(iMember+2);
//                    if(a!=null)
//                        get(iMember).samples[i] = (Float[]) a.getArray(); // +2 because first col is time and getArray is 1-based
//                }
//            }
//        }
////        MLObservation.Metadata[] mdGroup = new MLObservation.Metadata[groupNames.length];
//        private Object[] getMemberIDs() {
//            ArrayList<Object> result = new ArrayList<Object>();
//            for(GroupMember m : this)
//                result.add(m.event.getID());
//            return result.toArray();
//        }
//        /*
//         * Set the Group feature for this group in the specified Observation
//         */
//        MLObservation.GroupFeature observe(double t, int timeStep) {
//            if(!isMetadataCorrect(timeStep))
//                metadata = getMetadata(timeStep);
//            MLObservation.GroupFeature obs = new MLObservation.GroupFeature(t, metadata);
//            for(GroupMember m : this)
//                if(m.samples[timeStep]!=null)
//                    obs.setFeature(m.getName(), m.samples[timeStep]);
//            return obs;
//        }
//
//        private boolean isMetadataCorrect(int timeStep) {
//            if(metadata == null)
//                return false;
//            int numPresent = 0; // number of group members with non-null observed value at timeStep
//            for(GroupMember m : this) {
//                if(m.samples[timeStep] == null)
//                    continue;
//                ++numPresent;
//                if(!metadata.getFeatureNames().contains(m.getName()))
//                    return false;
//            }
//            // since we now know the metadata contains at least all the name of members currently present,
//            // if it is the same size it must match exactly.
//            if(metadata.getFeatureNames().size() != numPresent)
//                return false;
//            return true;
//        }
//
//        /**
//         * Creates a new metadata correct for the specified timestep
//         */
//        private MLObservation.Metadata getMetadata(int timeStep) {
//            MLObservation.Metadata md = new MLObservation.Metadata();
//            for(GroupMember m : this)
//                if(m.samples[timeStep] != null)
//                    md.add(m.getName(), m.event);
//            return md;
//        }
//    }
//
//    static class ProblemInstance
//    {
//        MLObservation.Metadata observationMetadata;
//
//        // the extractor for this problem instance.
//        MLFeatureExtractor extractor;
//
//        ProblemInstance(MLObservation.Metadata observationMetadata, MLFeatureExtractor extractor) throws SQLException
//        {
//            this.extractor = extractor;
//            this.observationMetadata = observationMetadata;
//        }
//
//        Set<Long> getEventIDs() {
//            Set<Long> result = new HashSet<Long>();
//            for(Event e : observationMetadata.getAllFeatureEvents())
//                result.add(e.getID());
//            return result;
//        }
//    }
//}
//
//
