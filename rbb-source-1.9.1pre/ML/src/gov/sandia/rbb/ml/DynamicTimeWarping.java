
package gov.sandia.rbb.ml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * This is a port of the "UCR Suite" Dynamic Time Warping algorithm and C code from:
 * http://www.cs.ucr.edu/~eamonn/UCRsuite.html
 * by Thanawin Rakthanmanon, Bilson Campana, Abdullah Mueen, Qiang Zhu, Jesin Zakaria, and Eamonn Keogh.
 *
 * It is generalized to n dimensions.
 *
 * @author rgabbot
 */
public class DynamicTimeWarping {

    static class LowerUpperLemire {
        private int r;
        // Deque<Integer>[] du, dl;
        ArrayList<Deque<Integer>> du, dl;
        Deque<double[]> data;
        int iResult;

        LowerUpperLemire(int r, int n) {
            this.r = r;
            data  = new Deque<double[]>(2*r+2);
            iResult = -1;
        }

        public boolean add(double[] x, double[] lower, double[] upper) {
            final int now = data.numAdded; // index the currently-added x
            data.pushBack(x);
            final int V = x.length;

            // now we know V, the dimensionality of the data.
            if(du == null) {
                du = new ArrayList<Deque<Integer>>();
                dl = new ArrayList<Deque<Integer>>();
                for(int i = 0; i < V; ++i) {
                    du.add(new Deque<Integer>(2*r+2));
                    dl.add(new Deque<Integer>(2*r+2));
                    du.get(i).pushBack(0);
                    dl.get(i).pushBack(0);
                }
            }

            if (now > r) // output result
            {
                ++iResult;
                for(int v = 0; v < V; ++v) {
                    upper[v] = data.zeroBased(du.get(v).front())[v];
                    lower[v] = data.zeroBased(dl.get(v).front())[v];
                }
            }

            // calculate each dimension of the data independently.
            if(now > 0)
            for(int v = 0; v < V; ++v) {
                if (data.recent(0)[v] > data.recent(1)[v]) // going up... adjust the upper
                {
                    du.get(v).popBack();
                    while (!du.get(v).empty() && data.recent(0)[v] > data.zeroBased(du.get(v).back())[v])
                        du.get(v).popBack();
                }
                else
                {
                    dl.get(v).popBack();
                    while(!dl.get(v).empty() && data.recent(0)[v] < data.zeroBased(dl.get(v).back())[v])
                        dl.get(v).popBack();
                }
                du.get(v).pushBack(now);
                dl.get(v).pushBack(now);
                if(now == 2 * r + 1 + du.get(v).front())
                    du.get(v).popFront();
                else if (now == 2 * r + 1 + dl.get(v).front())
                    dl.get(v).popFront();
            }

            return iResult >= 0;
        }

        public boolean drain(double[] lower, double[] upper) {
            if(iResult == data.numAdded-1) { // already computed all the bounds... at most, iResult indexes the last data element.
                return false;
            }
            ++iResult;
            final int V = data.recent(0).length;
            for(int v = 0; v < V; ++v) {
                upper[v] = data.zeroBased(du.get(v).front())[v];
                lower[v] = data.zeroBased(dl.get(v).front())[v];
                if(iResult+r-du.get(v).front() >= 2 * r)
                    du.get(v).popFront();
                if(iResult+r-dl.get(v).front() >= 2 * r)
                    dl.get(v).popFront();
            }
            return true;
        }

        double[] getData() {
            return data.zeroBased(iResult);
        }

        static void batch(double[][] t, int r, double[][] l, double[][] u) {
            LowerUpperLemire lul = new LowerUpperLemire(r, t.length);
            int j = 0;
            for(int i = 0; i < t.length; ++i)
                if(lul.add(t[i], l[j], u[j]))
                    ++j;
            for(; j < t.length; ++j)
                lul.drain(l[j], u[j]);
        }
    }

    /// Data structure for sorting the query
    static class Index implements Comparable<Index> {
        double[] value;
        double length;
        int    index;

        public Index(int index, double[] value) {
            this.index=index;
            this.value=value;
            length=0;
            for(double v : value)
                length += v*v;
        }

        @Override public int compareTo(Index o) {
            return o.length < length ? -1 : o.length == length ? 0 : 1; // high to low
        }
    };

    /**
     * Calculate quick lower bound
     * Usually, LB_Kim take time O(length) for finding top,bottom,first and last.
     * However, because of z-normalization the top and bottom cannot give significant benefits.
     * And using the first and last points can be computed in constant time.
     * The pruning power of LB_Kim is non-trivial, especially when the query is not long, say in length 128.
     */
    static double lb_kim_hierarchy(Deque<double[]> t, double[][] q, int n, ZNormalize z, double bsf)
    {
        final int v = q[0].length;

        /// 1 point at front and back
        double d, lb;
        double[] x0 = z.norm(t.window(n, 0));
        double[] y0 = z.norm(t.window(n, n-1));

        lb = dist(x0,q[0]) + dist(y0,q[n-1]);
        if (lb >= bsf)   return lb;

        /// 2 points at front
        double[] x1 = z.norm(t.window(n, 1));
        d = Math.min(dist(x1,q[0]), dist(x0,q[1]));
        d = Math.min(d, dist(x1,q[1]));
        lb += d;
        if (lb >= bsf)   return lb;

        /// 2 points at back
        double[] y1 = z.norm(t.window(n, n-2));
        d = Math.min(dist(y1,q[n-1]), dist(y0, q[n-2]) );
        d = Math.min(d, dist(y1,q[n-2]));
        lb += d;
        if (lb >= bsf)   return lb;

        /// 3 points at front
        double[] x2 = z.norm(t.window(n, 2));
        d = Math.min(dist(x0,q[2]), dist(x1, q[2]));
        d = Math.min(d, dist(x2,q[2]));
        d = Math.min(d, dist(x2,q[1]));
        d = Math.min(d, dist(x2,q[0]));
        lb += d;
        if (lb >= bsf)   return lb;

        /// 3 points at back
        double[] y2 = z.norm(t.window(n, n-3));
        d = Math.min(dist(y0,q[n-3]), dist(y1, q[n-3]));
        d = Math.min(d, dist(y2,q[n-3]));
        d = Math.min(d, dist(y2,q[n-2]));
        d = Math.min(d, dist(y2,q[n-1]));
        lb += d;

        return lb;
    }

    /**
     * LB_Keogh 1: Create Envelop for the query
     * Note that because the query is known, envelop can be created once at the begenining.
     *
     * Variable Explanation,
     * order : sorted indices for the query.
     * uo, lo: upper and lower envelops for the query, which already sorted.
     * data     : a circular array keeping the current fp.
     * cb    : (output) current bound at each position. It will be used later for early abandoning in DTW.
     *
     */
    static double lb_keogh_cumulative(int[] order, Deque<double[]> t, double[][] uo, double[][] lo, double[] cb, int n, ZNormalize z, double best_so_far)
    {
        final int v = t.window(0).length;
        double lb = 0;
        double [] x = new double[v];
        double [] y = new double[v];

        for (int i = 0; i < n && lb < best_so_far; i++)
        {
            z.norm(t.window(n, order[i]), x);
            for(int iv=0; iv < v; ++iv) {
                if(x[iv] > uo[i][iv])
                    y[iv] = uo[i][iv];
                else if(x[iv] < lo[i][iv])
                    y[iv] = lo[i][iv];
                else
                    y[iv] = x[iv];
            }
            final double d = dist(x,y);
            lb += d;
            cb[order[i]] = d;
        }
        return lb;
    }

    /**
     * LB_Keogh 2: Create Envelop for the data
     * Note that the envelops have been created (in main function) when each data point has been read.
     *
     * Variable Explanation,
     * tz: Z-normalized data
     * qo: sorted query
     * cb: (output) current bound at each position. Used later for early abandoning in DTW.
     * l,u: lower and upper envelop of the current data
     */
    static double lb_keogh_data_cumulative(int[] order, double[][] tz, double[][] qo, double[] cb, Deque<double[]> l, Deque<double[]> u, int n, ZNormalize z, double best_so_far)
    {
        final int v = tz[0].length;
        double lb = 0;
        double[] uu = new double[v];
        double[] ll = new double[v];
        double[] y = new double[v];

        for (int i = 0; i < n && lb < best_so_far; i++) {
            z.norm(u.window(n, order[i]), uu);
            z.norm(l.window(n, order[i]), ll);

            for(int iv=0; iv < v; ++iv) {
                if (qo[i][iv] > uu[iv])
                    y[iv] = uu[iv];
                else if(qo[i][iv] < ll[iv])
                    y[iv] = ll[iv];
                else
                    y[iv] = qo[i][iv];
            }
            final double d = dist(qo[i], y);
            lb += d;
            cb[order[i]] = d;
        }
        return lb;
    }

    static class ZNormalize implements Cloneable {
        private double[] sum;
        private double[] sumOfSq;
        private double[] mean;
        private double[] stddev;
        private Deque<double[]> data;
        private int n;
        private int addedSinceRefresh;
        boolean ready = false;

        /*
         * If false, the axes can be scaled independently.
         * So for example in x/y positional data a circle may not be round anymore.
         */
        boolean fixedAspect = true;

        public ZNormalize(int n) {
            data = new Deque<double[]>(n);
        }

        public void add(double[] x) {
            if(data.isFull())
                remove(data.window(0));
            data.pushBack(x);
            if(addedSinceRefresh==100000)
                refresh();
            else
                addImpl(x);
        }
        
        public double[] mean() {
            if(!ready)
                makeReady();
            return mean;
        }

        public double[] stddev() {
            if(!ready)
                makeReady();
            return stddev;
        }

        /*
         * store a normalized copy of x in y.
         */
        public void norm(double[] x, double[] y) {
            if(!ready)
                makeReady();
            for(int i = 0; i < mean.length; ++i)
                y[i] = (x[i]-mean[i]) / stddev[i];
        }

        /*
         * create a new normalized copy of x
         */
        public double[] norm(double[] x) {
            double[] y = new double[x.length];
            norm(x,y);
            return y;
        }

        /**
         * Inverse of norm - project already-normalized values into data coordinates.
         */
        public void inverseNorm(double[] y, double[] x) {
            if(!ready)
                makeReady();
            for(int i = 0; i < mean.length; ++i)
                x[i] = y[i] * stddev[i] + mean[i];
        }

        private void remove(double[] x) {
            ready=false;
            for(int i = 0; i < x.length; ++i) {
                sum[i] -= x[i];
                sumOfSq[i] -= x[i]*x[i];
            }
            --n;
        }

        private void addImpl(double[] x) {
            ready=false;
            if(sum==null) {
                sum = new double[x.length];
                sumOfSq = new double[x.length];
                mean = new double[x.length];
                stddev = new double[x.length];
                addedSinceRefresh=n=0;
            }
            for(int i = 0; i < x.length; ++i) {
                sum[i] += x[i];
                sumOfSq[i] += x[i]*x[i];
            }
            ++n;
            ++addedSinceRefresh;
        }

        /**
         * the incremental method of calculating the mean and stddev
         * accumulates floating point error; this restarts fresh.
         */
        private void refresh() {
            // System.err.println("refreshing");
            sum=null;
            for(int i = 0; i < data.size; ++i)
                addImpl(data.window(i));
        }

        private void makeReady() {
            final double s = 1.0/n;
            double radialVariance = 0.0;
            for(int i = 0; i < sum.length; ++i) {
                mean[i] = sum[i] * s;
                stddev[i] = sumOfSq[i]*s-mean[i]*mean[i]; // stddev[i] for the moment contains the variance rather than stddev.
                radialVariance += stddev[i];
            }

            if(fixedAspect) {
                final double radialStddev = Math.sqrt(radialVariance);
                for(int i = 0; i < sum.length; ++i)
                   stddev[i] = radialStddev;
            }
             else {
                for(int i = 0; i < sum.length; ++i)
                   stddev[i] = Math.sqrt(stddev[i]);
            }

            ready = true;
        }

        @Override
        public ZNormalize clone() {
            ZNormalize z = new ZNormalize(n);
            for(int i = 0; i < data.size; ++i)
                z.add(data.window(i));
            return z;
        }
    }

    /**
     * Read the next line as a comma-separated list of doubles.
     * Returns null at EOF
     */
    private static double[] readDoubles(BufferedReader is) throws Exception {
        final String s = is.readLine();
        if(s==null)
            return null;
        String[] a = s.split(",");
        double[] result = new double[a.length];
        for(int i = 0; i < a.length; ++i)
            result[i] = Double.parseDouble(a[i]);
        return result;
    }

    public static void main(String[] args) throws Exception {
//
//        Deque<Integer> d = new Deque<Integer>(3);
//        for(int i = 0; i < 10; ++i) {
//            d.pushBack(i);
//            System.out.println((i>0?""+d.window(2,0):"") + d.window(2,1));
//        }
//        int x = 1;
//        if(x>0)
//        throw new Exception("done");

        /// If not enough input, display an error.
        final String usage = "DynamicTimeWarping <dataFile> <warpWindow e.g. 0.1> <maxError e.g. 1> <queryFile> [<queryFile2...>]";
        if (args.length != 3)
            throw new Exception(usage);

        final double warpWindow = Double.parseDouble(args[1]);
        final double maxError = Double.parseDouble(args[2]);
        double[] v;


        //// read queries
        ArrayList<double[][]> queries = new ArrayList<double[][]>();
        ArrayList<String> queryNames = new ArrayList<String>();
        for(int i = 3; i < args.length; ++i) {
            final String fileName = args[i];
            BufferedReader qp = new BufferedReader(new FileReader(new File(fileName)));   /// query file pointer
            ArrayList<double[]> query = new ArrayList<double[]>();
            while((v=readDoubles(qp))!=null)
                query.add(v);
            qp.close();
            queries.add(query.toArray(new double[0][]));
            queryNames.add(fileName);
        }

        //// instantiate DTW
        DynamicTimeWarping dtw = new DynamicTimeWarping(queries.toArray(new double[0][][]), queryNames.toArray(new String[0]), warpWindow, maxError);

        //// open data
        BufferedReader fp = new BufferedReader(new FileReader(new File(args[0])));

        //// feed data to dtw
        while((v=readDoubles(fp)) != null)
            dtw.observe(v);
        while(dtw.observe(null))
            ; // do-nothing.. just draining the final results.

        fp.close();

        dtw.stats.print();
    }

    class Stats {
        long startTime;

        /*
         * How many times each pruning method came into play
         */
        int kim = 0,keogh = 0, keogh2 = 0;

        Stats() {
            startTime = System.currentTimeMillis();
        }

        void print() {
            // System.err.println("Location : " + loc);
            System.err.println("Distance : " + Math.sqrt(bsf));
            System.err.println("Data Scanned : " + numObs);
            System.err.println("Total Execution Time : "+((System.currentTimeMillis()-startTime)/1000.0) +" sec");

            System.err.println("");
            System.err.println("Pruned by LB_Kim    : " + ((double) kim / numObs)*100);
            System.err.println("Pruned by LB_Keogh  : " + ((double) keogh / numObs)*100);
            System.err.println("Pruned by LB_Keogh2 : " + ((double) keogh2 / numObs)*100);
            System.err.println("DTW Calculation     : " + (100-(((double)kim+keogh+keogh2)/numObs*100)));
        }
    }


    private static double[][] newArray(int n, int m) {
        double[][] result = new double[n][];
        for(int i =0; i < n; ++i)
            result[i] = new double[m];
        return result;
    }

    private static void setZero(double[] v) {
        for(int i = 0; i < v.length; ++i)
            v[i] = 0;
    }

    Deque<double[]> t;
    Deque<double[]> l_buff, u_buff;
    double[][] tz;
    int numObs;
    int V; // dimensionality of observations
    double[] cb, cb1, cb2;
    Stats stats;
    LowerUpperLemire lul;
    int maxQueryLength;
    
    /*
     *  best-so-far (lower is better)
     */
    double bsf;

    Query[] queries;

    /**
     * Contains a list of tentative matches, ordered by END time
     * (not start time!) which is significant if the queries have
     * different lengths.
     */
    LinkedList<Match> matches;

    /**
     * Caller-specified threshold for reporting results.  Is not applied if null.
     */
    Double maxError;

    private class Query {
        double[][] samples; // normalized query
        double[][] qo, lo, uo;
        int length;
        int warpWindow;
        int[] order;
        String name;
        ZNormalize queryZ; // used to z-normalize the query.  Doesn't change after the constructor.
        ZNormalize dataZ; // used to z-norm data to be compared to this query, always reflecting the most recent window.  Could be shared with other queries of equal length.


        Query(String name, double[][] query, double warpWindow0) {
            length=query.length;
            this.name = name;

            if (warpWindow0 <= 1)
                this.warpWindow = (int)Math.floor(warpWindow0 * length);
            else
                this.warpWindow = (int)Math.floor(warpWindow0);
        
           samples = newArray(length,V);
           qo = newArray(length,V); // the normalized query, sorted in descending order of magnitude.

            // z-normalize the query
            queryZ = new ZNormalize(length);
            for(double[] x : query)
                queryZ.add(x);
            for(int i = 0; i < length; ++i)
                queryZ.norm(query[i], samples[i]);

            // Create envelop of the query: lower envelop, l, and upper envelop, u
            double[][] u = newArray(length,V);
            double[][] l = newArray(length,V);
            LowerUpperLemire.batch(samples, this.warpWindow, l, u);

            // you can make a nice plot to illustrate the Lemire envelope with this...
//            System.out.println("x and its envelope");
//            for(int i = 0; i < length; ++i)
//                System.out.println(i + " " + samples[i][0] + " " + l[i][0] + " " + u[i][0]);
//
//            System.out.println("y and its envelope");
//            for(int i = 0; i < length; ++i)
//                System.out.println(i + " " + samples[i][1] + " " + l[i][1] + " " + u[i][1]);


            /// Sort the query one time by abs(z-norm(samples[numSamples]))
            Index[] Q_tmp = new Index[length];
            for(int i = 0; i<length; i++)
                Q_tmp[i] = new Index(i, samples[i]);
            Arrays.sort(Q_tmp);

            /// also create another arrays for keeping sorted envelop
            uo = newArray(length,V);
            lo = newArray(length,V);
            order = new int[length]; // sorted order of the query
            for(int i=0; i<length; i++)
            {
                int o = Q_tmp[i].index;
                order[i] = o;
                qo[i] = samples[o];
                uo[i] = u[o];
                lo[i] = l[o];
            }

            // dataZ will be used on the data to be searched.
            dataZ = new ZNormalize(length);
            lul = new LowerUpperLemire(this.warpWindow, length);
        }
    }

    /*
     * Each queries[i] is a pattern (timeseries) to be searched for.  >= 1 pattern must be provided.
     * Each queries[i][j] is an (multi-dimensional, e.g. 2d) observation of a pattern.  Each pattern must have at least 3 observations.
     * Each queries[i][j][k] is a single dimension of an observation, e.g. the x-component.
     *<p>
     * maxErrorRate squelches bad matches.  It can be null, but in that case
     * (or if it is simply too high a number) there will be
     * bogus results in between the "real" ones.
     *
     */
    public DynamicTimeWarping(double[][][] queries, String[] queryNames, double warpWindow, Double maxErrorRate) throws Exception {
        // check args
        if(queries == null || queries.length == 0)
            throw new Exception("DynamicTimeWarping error: 'queries' is null or empty");
        if(queryNames == null || queryNames.length != queries.length)
            throw new Exception("DynamicTimeWarping error: 'queriesNames' must be the same length as 'queries'");
        for(int i = 0; i < queries.length; ++i) {
            if(queryNames[i] == null)
                throw new Exception("DynamicTimeWarping error: queryNames["+i+"] is null");
            if(queries[i] == null)
                throw new Exception("DynamicTimeWarping error: query["+i+"] ("+queryNames[i]+") is null");
            if(queries[i].length < 3)
                throw new Exception("DynamicTimeWarping error: the length of query["+i+"] ("+queryNames[i]+") must be >= 3 but is only " + queries[i].length);
        }

        V = queries[0][0].length; // dimensionality of observation vectors.  Must be the same for all queries[i][j]
        bsf = Double.MAX_VALUE; // best-so-far (lower is better)        
        stats = new Stats();
        matches = new LinkedList<Match>();
        this.maxError = maxErrorRate;

        maxQueryLength = 0;
        this.queries = new Query[queries.length];
        for(int i = 0; i < queries.length; ++i) {
            this.queries[i] = new Query(queryNames[i], queries[i], warpWindow);
            maxQueryLength = Math.max(maxQueryLength, queries[i].length);
        }

        cb = new double[maxQueryLength];
        cb1 = new double[maxQueryLength];
        cb2 = new double[maxQueryLength];

        t = new Deque<double[]>(maxQueryLength);
        l_buff = new Deque<double[]>(maxQueryLength);
        u_buff = new Deque<double[]>(maxQueryLength);
        tz = newArray(maxQueryLength,V);

        numObs = 0;
    }
    
    public boolean observe(double[] d) {

        double[] lower = getNextBuf(l_buff);
        double[] upper = getNextBuf(u_buff);
        if(d == null && !lul.drain(lower, upper)) {
            return false; // all done.
        }
        if(d != null && !lul.add(d, lower, upper))
            return true; // still priming the pump
        l_buff.pushBack(lower);
        u_buff.pushBack(upper);
        d = lul.getData(); // retrieves previous data corresponding to the newly produced upper/lower bounds.

        /// data is a circular array for keeping current data
        t.pushBack(d);

        ++numObs;

        for(int iq=0; iq < queries.length; ++iq)
            observeForQuery(queries[iq]);

       // System.out.println(numObs + " " + bsf);

        return true;
    }

    private void observeForQuery(Query q) {
        q.dataZ.add(t.recent(0));

        if(numObs < q.length)
            return; // don'data have enough history yet...

        // the lower bound is the best score (i.e. min error) attained within 
        // now - length.
        // search back from the tail
        double lowestErrorRate = Double.MAX_VALUE;
        if(maxError!=null)
            lowestErrorRate = maxError;
        if(!matches.isEmpty()) {
            ListIterator<Match> it = matches.listIterator(matches.size());
            while(it.hasPrevious()) {
                Match m = it.previous();
                if(m.endPos < numObs-q.length)
                    break;
                if(m.errorRate() < lowestErrorRate)
                    lowestErrorRate = m.errorRate();
            }
        }

        if(lowestErrorRate == Double.MAX_VALUE)
            bsf = Double.MAX_VALUE;
        else
          bsf = lowestErrorRate * q.length;

        // Use a constant lower bound to prune the obvious subsequence
        final double lb_kim = lb_kim_hierarchy(t, q.samples, q.length, q.dataZ, bsf);

        if (lb_kim >= bsf) {
            stats.kim++;
            return; ///////// pruned
        }

        /// Use a linear time lower bound to prune; z_normalization of data will be computed on the fly.
        /// uo, lo are envelop of the query.
        final double lb_k = lb_keogh_cumulative(q.order, t, q.uo, q.lo, cb1, q.length, q.dataZ, bsf);
        if (lb_k >= bsf) {
            stats.keogh++;
            return; ///////// pruned
        }

        // Take another linear time to compute z_normalization of data.
        // TODO: this could be shared between the queries.
        for(int k=0; k < q.length;k++)
            q.dataZ.norm(t.window(q.length, k), tz[k]);

        /// Use another lb_keogh to prune
        /// qo is the sorted query. tz is unsorted z_normalized data.
        /// l_buff, u_buff are big envelop for all data in this chunk
        final double lb_k2 = lb_keogh_data_cumulative(q.order, tz, q.qo, cb2, l_buff, u_buff, q.length, q.dataZ, bsf);
        if (lb_k2 >= bsf) {
            stats.keogh2++;
            return; ///////// pruned
        }

        /// Choose better lower bound between lb_keogh and lb_keogh2 to be used in early abandoning DTW
        /// Note that cb and cb2 will be cumulative summed here.
        if (lb_k > lb_k2)
        {
            cb[q.length-1]=cb1[q.length-1];
            for(int k=q.length-2; k>=0; k--)
                cb[k] = cb[k+1]+cb1[k];
        }
        else
        {
            cb[q.length-1]=cb2[q.length-1];
            for(int k=q.length-2; k>=0; k--)
                cb[k] = cb[k+1]+cb2[k];
        }

        // Compute DTW and early abandoning if possible
        final double dist = dtw(tz, q.samples, cb, q.length, q.warpWindow, bsf);

        if( dist < bsf ) {
            bsf = dist;
            matches.add(new Match(q, numObs-1, bsf));
        }

        return;
    }

    /**
     * Get results.  A result is a Match for a time period in which no better Match
     * was found.
     * <p>
     * If the result is other than null, getResult may return another result if
     * called immediately.
     */
    public Match getResult(boolean moreToCome) {

        // see if there is a result that can be reported now.

        // A result is overshadowed by another if they overlap in time and the other has a lower error.
        // In case of ties, the one earlier in the list wins.

        // It can be reported if it meets these conditions:
        // 1) It is not shadowed by anything after it.
        // 2) It is too old to be shadowed by anything that may yet arrive (if moreToCome is true)
        // 3) It is not shadowed by anything before it.

        // Note that a Match cannot be overshadowed by something that comes
        // before it in the list, because in this case it is never added in the first place.

        // However if a Match is overshadowed by something that comes after it in
        // the list, it may yet be reported, because the other Match that overshadows
        // it may be overshadowed by something else and so get removed at some point.

        // So, we are just looking for the first Match in the list that is not overshadowed
        // by anything that comes after or may come after.

        // In this case finalize it and remove all other results that overlap in time

        // If we find something that overshadows our current candiate, it is our
        // new candidate.  Results in between that were overshadowed by the current
        // candidate may yet become reportable but not until/unless the current
        // candidate is removed.

        ListIterator<Match> it = matches.listIterator();
        if(!it.hasNext())
            return null;

        Match candidate = it.next();
        if(moreToCome && candidate.endPos + maxQueryLength > numObs)
            return null;

        while(it.hasNext()) {
            Match other = it.next();
            if(other.endPos > candidate.endPos + maxQueryLength)
                break; // we have worked past all others that could possibly overshadow the candidate.

            if(other.startPos() > candidate.endPos)
                continue; // this other does not overlap in time, although other longer queries may yet do so.

            if(other.errorRate() < candidate.errorRate()) { // the current candidate is overshadowed by this other.
                candidate = other;
                if(moreToCome && candidate.endPos + maxQueryLength > numObs)
                    return null; // early exit... we reached a candidate for which the jury is still out.
            }
        }

        // if we get here, we have a final result.

        // report it.
        // System.err.println(candidate);

        // Remove all overshadowed by the result, i.e. everything that overlaps with it.
        // These must be before the candidate in list:
        //   If they were after and had a lower score they would never be found or added to the list
        //   If they were after and had a higher score they would have overshadowed the candidate in the loop above.
        it = matches.listIterator();
        while(it.hasNext()) {
            Match other = it.next();
            if(other.endPos > candidate.endPos)
                break;
            if(other.endPos < candidate.startPos())
                continue;
            it.remove();
        }

        return candidate;
    }

    public static class Match {
        Query query;
        long endPos;
        double error;
        /*
         * copy of query.dataZ at time of match.  Must make a copy since query.dataZ continues to evolve as more data is presented.
         */
        ZNormalize dataZ; 

        public double errorRate() { return error / query.length; };
//        public double errorRate() { return error; };
        public long startPos() { return endPos - query.length + 1; };
        public long endPos() { return endPos; };
        public String queryName() { return query.name; };

        public int getNumSamples() { return query.length; }
        public void getQuerySample(int i, double[] x) { dataZ.inverseNorm(query.samples[i], x); }

        /**
         * How much is the data in the match scaled up (or down) compared to the query that was matched.
         */
        public Double getScale() {
            dataZ.makeReady();
            query.queryZ.makeReady();
            return dataZ.stddev[0] / query.queryZ.stddev[0];
        }

        Match(Query query, long endPos, double error) {
            this.query = query;
            this.endPos = endPos;
            this.error = error;
            this.dataZ = query.dataZ.clone();
        }

        @Override
        public String toString() {
            return query.name + " matches at start=" + startPos() + " end=" + endPos() + "; error is " + error + " (" + error/query.length + " per sample)";
        }
    }


    /**
     * Euclidian distance squared.
     */
    static double dist(double[] a, double[] b) {
        double sum = 0.0;
        for(int i = 0; i < a.length; ++i) {
            final double d = b[i]-a[i];
            sum += d*d;
        }
        return sum;
    }

    /**
     * Calculate Dynamic Time Warping distance
     * D,Q: data and query, respectively
     * cb : cummulative bound used for early abandoning
     *  warpWindow  : size of Sakoe-Chiba warpping band
     */
    static double dtw(double[][] D, double[][] Q, double[] cb, int m, int r, double bsf)
    {
        int i,j,k;
        double x,y,z,min_cost;

        /// Instead of using matrix of size O(length^2) or O(mr), we will reuse two array of size O(warpWindow).
        double[] cost = new double[2*r+1];
        double[] cost_prev = new double[2*r+1];
        for(k=0; k<2*r+1; k++)
            cost_prev[k] = cost[k] = Double.MAX_VALUE;

        for (i=0; i<m; i++)
        {
            k = Math.max(0,r-i);
            min_cost = Double.MAX_VALUE;

            for(j=Math.max(0,i-r); j<=Math.min(m-1,i+r); j++, k++)
            {
                /// Initialize all row and column
                if ((i==0)&&(j==0))
                {
                    cost[k]=dist(D[0],Q[0]);
                    min_cost = cost[k];
                    continue;
                }

                if ((j-1<0)||(k-1<0))     y = Double.MAX_VALUE;
                else                      y = cost[k-1];

                if ((i-1<0)||(k+1>2*r))   x = Double.MAX_VALUE;
                else                      x = cost_prev[k+1];

                if ((i-1<0)||(j-1<0))     z = Double.MAX_VALUE;
                else                      z = cost_prev[k];

                /// Classic DTW calculation
                cost[k] = Math.min(Math.min( x, y) , z) + dist(D[i],Q[j]);

                /// Find minimum cost in row for early abandoning (possibly to use column instead of row).
                if (cost[k] < min_cost)
                   min_cost = cost[k];

            }

            /// We can abandon early if the current cummulative distace with lower bound together are larger than bsf
            if (i+r < m-1 && min_cost + cb[i+r+1] >= bsf)
                return min_cost + cb[i+r+1];

            /// Move current array to previous array.
            double[] cost_tmp = cost;
            cost = cost_prev;
            cost_prev = cost_tmp;
        }
        k--;

        /// the DTW distance is in the last cell in the matrix of size O(length^2) or at the middle of our array.
        double final_dtw = cost_prev[k];
        return final_dtw;
    }

    /**
     * Calculate Dynamic Time Warping distance
     * D,Q: data and query, respectively
     * cb : cummulative bound used for early abandoning
     *  warpWindow  : size of Sakoe-Chiba warpping band
     */
    static double dtw2(double[][] D, double[][] Q, double[] cb, int r, double bsf)
    {
        int i,j,k;
        double x,y,z,min_cost;

        /// Instead of using matrix of size O(length^2) or O(mr), we will reuse two array of size O(warpWindow).
        double[] cost = new double[2*r+1];
        double[] cost_prev = new double[2*r+1];
        for(k=0; k<2*r+1; k++)
            cost_prev[k] = cost[k] = Double.MAX_VALUE;

        for (i=0; i < D.length; i++)
        {
            k = Math.max(0,r-i);
            min_cost = Double.MAX_VALUE;

            for(j=Math.max(0,i-r); j <= Math.min(Q.length-1,i+r); j++, k++)
            {
                /// Initialize all row and column
                if ((i==0)&&(j==0))
                {
                    cost[k]=dist(D[0],Q[0]);
                    min_cost = cost[k];
                    continue;
                }

                if ((j-1<0)||(k-1<0))     y = Double.MAX_VALUE;
                else                      y = cost[k-1];

                if ((i-1<0)||(k+1>2*r))   x = Double.MAX_VALUE;
                else                      x = cost_prev[k+1];

                if ((i-1<0)||(j-1<0))     z = Double.MAX_VALUE;
                else                      z = cost_prev[k];

                /// Classic DTW calculation
                cost[k] = Math.min(Math.min( x, y) , z) + dist(D[i],Q[j]);

                /// Find minimum cost in row for early abandoning (possibly to use column instead of row).
                if (cost[k] < min_cost)
                   min_cost = cost[k];

            }

            /// We can abandon early if the current cummulative distace with lower bound together are larger than bsf
            if (i+r < D.length - 1 && min_cost + cb[i+r+1] >= bsf)
                return min_cost + cb[i+r+1];

            /// Move current array to previous array.
            double[] cost_tmp = cost;
            cost = cost_prev;
            cost_prev = cost_tmp;
        }
        k--;

        /// the DTW distance is in the last cell in the matrix of size O(length^2) or at the middle of our array.
        double final_dtw = cost_prev[k];
        return final_dtw;
    }

    /**
     * Calculate Dynamic Time Warping distance
     * D,Q: data and query, respectively
     * cb : cummulative bound used for early abandoning
     *  warpWindow  : size of Sakoe-Chiba warpping band
     */
//    static double dtw2(double[][] D, double[][] Q, double[] cb, int r, double bsf)
//    {
//        // allocate cost array
//        // each cost[q][d] is the min total cost of pairings from D[0] to Q[0]
//        double[][] cost = new double[Q.length][];
//        for(int q = 0; q < Q.length; ++q)
//            cost[q] = new double[D.length];
//
//        for(int q=0; q < Q.length; ++q)
//        {
//            for(int d=0; d < D.length; ++d)
//            {
//                /// Initialize all row and column
//                if ((i==0)&&(j==0))
//                {
//                    cost[k]=dist(D[0],Q[0]);
//                    min_cost = cost[k];
//                    continue;
//                }
//
//                if ((j-1<0)||(k-1<0))     y = Double.MAX_VALUE;
//                else                      y = cost[k-1];
//
//                if ((i-1<0)||(k+1>2*r))   x = Double.MAX_VALUE;
//                else                      x = cost_prev[k+1];
//
//                if ((i-1<0)||(j-1<0))     z = Double.MAX_VALUE;
//                else                      z = cost_prev[k];
//
//                /// Classic DTW calculation
//                cost[k] = Math.min(Math.min( x, y) , z) + dist(D[i],Q[j]);
//
//                /// Find minimum cost in row for early abandoning (possibly to use column instead of row).
//                if (cost[k] < min_cost)
//                   min_cost = cost[k];
//
//            }
//
//            /// We can abandon early if the current cummulative distace with lower bound together are larger than bsf
//            if (i+r < m-1 && min_cost + cb[i+r+1] >= bsf)
//                return min_cost + cb[i+r+1];
//
//            /// Move current array to previous array.
//            double[] cost_tmp = cost;
//            cost = cost_prev;
//            cost_prev = cost_tmp;
//        }
//        k--;
//
//        /// the DTW distance is in the last cell in the matrix of size O(length^2) or at the middle of our array.
//        double final_dtw = cost_prev[k];
//        return final_dtw;
//    }

    /**
     * circular array
     */
    static class Deque<T>
    {
        Deque(int capacity) {
            size = 0;
            numAdded = 0;
            dq = (T[]) new Object[capacity];
            f = 0;
            r = capacity-1;
        }
        int capacity() { return dq.length; }

        boolean isFull() { return size == dq.length; };

        /**
         * Insert to the queue at the back.
         * Evicts the element at the front if size is already at capacity
         */
        void pushBack(T v) {
            if(size==dq.length)
                popFront();
            dq[r] = v;
            r--;
            if(r < 0)
                r = capacity()-1;
            size++;
            numAdded++;
        }

        /// Delete the last element from queue
        void popBack()
        {
            r = (r+1)%capacity();
            size--;
        }

        /**
         * Get the value at the current position of the circular queue
         */
        T front() {
            int aux = f - 1;

            if (aux < 0)
                aux = capacity()-1;
            return dq[aux];
        }

        /**
         * Get the value at the last position of the circular queue
         */
        T back() {
            int aux = (r+1) % capacity();
            return dq[aux];
        }

        /// Check whether or not the queue is empty
        boolean empty() {
            return size == 0;
        }

        /**
         *  Delete the current (front) element from queue
         */
        void popFront()
        {
            f--;
            if (f < 0)
                f = capacity()-1;
            size--;
        }

        /**
         * Get the ith value in the sliding window, 0 is oldest.
         */
        T window(int i) {
            assert(i >= 0 && i < size);
            int aux = f - 1 - i;
            while(aux < 0)
                aux += capacity();
            return dq[aux];
        }

        /**
         * Get the ith value in a sliding window whose size is less than or equal to the capacity of this deque
         */
        T window(int windowSize, int i) {
            assert(windowSize <= capacity());
            assert(i >= 0 && i < windowSize);
            return recent(windowSize-i-1);
        }

        /**
         * Get the ith value ever added 0 is oldest.
         * i must be at least numAdded-size, since after that i is overwritten.
         */
        T zeroBased(int i) {
            if(i >= numAdded)
                throw new IllegalArgumentException();
            assert(i < numAdded);
            assert(i >= numAdded-size); // if this fails the value is already gone.
            return window(i-(numAdded-size));
        }

        /*
         * Index from most recently added; 0 = most recent.
         */
        T recent(int i) {
            assert(i>=0 && i < size);
            return window(size-i-1);
        }

        T[] dq;
        int size, numAdded;
        int f,r;
    }

    private double[] getNextBuf(Deque<double[]> d) {
        if(d.isFull())
            return d.window(0); // recycle oldest
        else
            return new double[V];
    }
}
