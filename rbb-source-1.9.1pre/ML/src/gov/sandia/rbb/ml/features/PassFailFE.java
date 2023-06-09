
package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.MLObservationSequence;

/**
 * This is a base class for Feature Extractor classes that will admit an entire
 * timeseries if any of the observations pass some test.
 *<p>
 * For example, "all timeseries that pass through this bounding box at some point", or
 * "all timeseries with at least N samples", or
 * "all timeseries with no samples for at least N seconds."
 *<p>
 * To detect a pass or fail condition, a derived class should override one or more
 * of init, observe, or done and call this base class super.x(...) in all cases.
 *<p>
 * Observations will be buffered (and no subsequent feature extractors called) until/unless
 * pass() or fail() is called.  If pass() is called, any buffered observations and all
 * subsequent observations will be propagated to the rest of the chain.  If fail()
 * is called, any buffered observations are discarded and the rest of the chain is not called.
 * Any subsequent calls to pass() or fail() during the same problem instance are ignored.
 *
 * @author rgabbot
 */
public class PassFailFE extends MLFeatureExtractor {

    /**
     * Base class for gates that pass or fail based on an n-dimensional bounding box.
     * Add more constructors to support higher dimensions.
     */
    public static class BBox extends PassFailFE {
        Float[] min, max;
        String input;
        public BBox(String input, Float min, Float max, MLFeatureExtractor next) {
            super(next);
            this.input = input;
            this.min = new Float[] { min };
            this.max = new Float[] { max };
        }
        public BBox(String input, Float minX, Float minY, Float maxX, Float maxY, MLFeatureExtractor next) {
            super(next);
            this.input = input;
            this.min = new Float[] { minX, minY };
            this.max = new Float[] { maxX, maxY };
        }
        boolean inside(MLObservation ob) {
            Float[] x = ob.getFeatureAsFloats(input);
            if(x==null)
                return false;
            for(int v = 0; v < x.length; ++v) {
                if(min[v] != null && x[v] < min[v])
                    return false;
                if(max[v] != null && x[v] > max[v])
                    return false;
            }
            return true;
        }
    }

    /**
     * Accept timeseries that (at some point) go into a specified (hyper-) rectangle
     * Any of the min or max extents can be null, in that case it is unbounded in that direction.
     */
    public static class EntersArea extends BBox {
        public EntersArea(String input, Float min, Float max, MLFeatureExtractor next) {
            super(input, min, max, next);
        }
        public EntersArea(String input, Float minX, Float minY, Float maxX, Float maxY, MLFeatureExtractor next) {
            super(input, minX, minY, maxX, maxY, next);
        }
        @Override
        public void observe(MLObservation obs) throws Exception {
            if(inside(obs))
                pass();
            super.observe(obs);
        }
    }

    /**
     * Accept the observation sequence only if the first non-null sample is inside the specified bounding box.
     */
    public static class StartsIn extends BBox {
        public StartsIn(String input, Float min, Float max, MLFeatureExtractor next) {
            super(input, min, max, next);
        }
        public StartsIn(String input, Float minX, Float minY, Float maxX, Float maxY, MLFeatureExtractor next) {
            super(input, minX, minY, maxX, maxY, next);
        }
        @Override
        public void observe(MLObservation obs) throws Exception {
            super.observe(obs);
            if(obs.getFeatureAsFloats(input) == null)
                return; // don't decide on basis of null feature
            if(state != State.UNDECIDED)
                return; // decide only the first time
            if(inside(obs))
                pass();
            else
                fail();
        }
    }

    /**
     * Accept the observation sequence only if the first last-null sample is inside the specified bounding box.
     */
    public static class EndsIn extends BBox {
        public EndsIn(String input, Float min, Float max, MLFeatureExtractor next) {
            super(input, min, max, next);
        }
        public EndsIn(String input, Float minX, Float minY, Float maxX, Float maxY, MLFeatureExtractor next) {
            super(input, minX, minY, maxX, maxY, next);
        }
        @Override
        public void done(double time) throws Exception {
            for(int i = 0; i < obSeq.size(); ++i) { // search for final non-null observation and make immediate decision on it.
                MLObservation ob = obSeq.getNewest(i);
                if(ob.getFeatureAsFloats(input) == null)
                    continue;
                if(inside(ob))
                    pass();
                else
                    fail();
                break;
            }

            super.done(time);
        }
    }

    /**
     * Accept observation sequence if the time between any pair of successive observations exceed the specified time threshold.
     */
    public static class ObservationGap extends PassFailFE {
        Double gap;
        Double prevTime;
        public ObservationGap(double gap, MLFeatureExtractor next) {
            super(next);
            this.gap = gap;
        }

        @Override
        public void init(double time, Metadata md) throws Exception {
            super.init(time, md);
            prevTime = null;
        }

        @Override
        public void observe(MLObservation obs) throws Exception {
            super.observe(obs);
            if(prevTime != null &&
                    obs.getTime() - prevTime > gap)
                pass();
            prevTime = obs.getTime();
        }
    }

    /**
     * Exclude the observation sequence if any of the inputs have a specified tagset
     */
    public static class ExcludeTags extends PassFailFE {
        Tagset tags;
        public ExcludeTags(Tagset tags, MLFeatureExtractor next) {
            super(next);
            this.tags = tags;
        }
        @Override
        public void init(double time, Metadata md) throws Exception {
            super.init(time, md);
            for(Event ev : md.getAllFeatureEvents()) {
                if(tags.isSubsetOf(ev.getTagset())) {
                    System.err.println("Excluding " + ev.getTagset());
                    fail();
                    return;
                }
            }
            System.err.println("ExcludeTags: nothing matched, so not excluding...");
            pass(); // don't get here unless nothing matched.
        }
    }


    MLObservationSequence obSeq;
    State state;
    Double initTime;
    Metadata metadata;

    enum State { UNDECIDED, PASS, FAIL, INACTIVE };

    public PassFailFE(MLFeatureExtractor nextFeatureExtractor) {
        super(nextFeatureExtractor);
        state = State.INACTIVE;
    }

    /**
     * Can be overriden to determine the fate of the timeseries immediately,
     * e.g. based on tag values.  The override must call
     * super.init(...) BEFORE calling pass() or fail()
     */
    @Override
    public void init(double time, Metadata md) throws Exception {
        state = State.UNDECIDED;
        initTime = time;
        metadata = md;
        obSeq=null;
    }

    /**
     * Can be overriden to determine the fate of the timeseries based on
     * observed values.
     * <p>
     * super.observe(...) should be called by the override.
     * (It doesn't matter whether super.observe is called before or after the
     * override calls pass() or fail())
     */
    @Override
    public void observe(MLObservation obs) throws Exception {
        if(state == State.PASS)
            super.observe(obs);
        else if(state == State.UNDECIDED) {
            if(obSeq == null)
                obSeq = new MLObservationSequence(null, null, metadata);
            obSeq.addObservation(obs);
        }
    }   

    /**
     * Can be overriden to determine the fate of the timeseries at the end,
     * e.g. based on the number of observations received.
     * <p>
     * If the override calls pass() it must do so BEFORE calling
     * super.done(time)
     */
    @Override
    public void done(double time) throws Exception {
        if(state == State.PASS)
            super.done(time);        
        state = State.INACTIVE;
    }

    public void pass() throws Exception {
        if(state == State.INACTIVE)
            throw new Exception("PassFailFE.pass() can only be called between init() and subsequent done()");

        if(state != State.UNDECIDED) // can only decide once per sequence of observations
            return;
        
        state = State.PASS;
        
        super.init(initTime, metadata);

        if(obSeq != null) {
            for(int i = 0; i < obSeq.size(); ++i)
                super.observe(obSeq.getOldest(i));
            obSeq = null;
        }
    }
    
    public void fail() throws Exception {
        if(state == State.INACTIVE)
            throw new Exception("PassFailFE.fail() can only be called between init() and subsequent done()");
        if(state != State.UNDECIDED) // can only decide once per sequence of observations
            return;
        state = State.FAIL;
        obSeq = null;
    }

}
