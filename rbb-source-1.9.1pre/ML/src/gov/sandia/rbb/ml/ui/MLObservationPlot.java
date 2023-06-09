
package gov.sandia.rbb.ml.ui;

import java.util.Arrays;
import gov.sandia.rbb.Event;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Paint;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import org.jfree.chart.axis.AxisState;
import org.jfree.chart.plot.PlotRenderingInfo;
import org.jfree.chart.plot.XYPlot;
import org.jfree.ui.RectangleEdge;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.ml.MLObservationSequence;
import gov.sandia.rbb.ml.RBBML;
import gov.sandia.rbb.ml.RBBML.MLPart;
import java.util.ArrayList;
import org.jfree.chart.ChartMouseEvent;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import gov.sandia.rbb.tools.RBBSelection;
import gov.sandia.rbb.ui.RBBEventUI;
import gov.sandia.rbb.ui.RBBReplayControl;
import java.awt.BasicStroke;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.imageio.ImageIO;
import javax.swing.JOptionPane;
import org.jfree.chart.ChartMouseListener;
import org.jfree.chart.annotations.XYImageAnnotation;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.entity.AxisEntity;
import org.jfree.chart.entity.XYItemEntity;
import org.jfree.chart.renderer.LookupPaintScale;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.PaintScaleLegend;
import org.jfree.data.Range;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * A chart panel that displays sequences of observations.
 *
 * @author rgabbot
 */
public class MLObservationPlot extends XYPlot {

    RBBML rbbml;
    RBBEventUI selectionChange; // , timeSelection;
    Tagset selectionTemplate;
    RBBReplayControl replayControl;
    BufferedImage currentValueMarkerImage, currentSelectedValueMarkerImage;
    RBBSelection selections;

    public MLObservationPlot(final RBBML rbbml, MLObservationsXYZDataset dataset) throws Exception {
        super(dataset,
          new NumberAxis(dataset.getAxisString(0)) {{ setAutoRangeIncludesZero(false); setLabelPaint(Color.BLUE); }}, // blue color is supposed to imply to the user they are clickable
          new NumberAxis(dataset.getAxisString(1)) {{ setAutoRangeIncludesZero(false);  setLabelPaint(Color.BLUE); }},
                  null);
          this.rbbml = rbbml;

          if(!dataset.isXYZ())
              setRenderer(new XYLineAndShapeRenderer(true,true));
          else
            this.setRenderer(
              new XYLineAndShapeRenderer(true, true) {
                { setUseFillPaint(true); }
                @Override
                public Paint getItemPaint(int row, int column) {
                    return zPaintScale.getPaint(getDataset().getZValue(row, column));
                    // return Color.BLACK;
                }
                @Override
                public Paint getItemFillPaint(int row, int column) {
                    return getItemPaint(row,column);
                }
                @Override
                public Paint getSeriesPaint(int row) {
                    return Color.BLACK;
                }
          });

          currentValueMarkerImage = ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/BlackRing.png"));
          currentSelectedValueMarkerImage = ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/GoldRing.png"));

          ////// bolding of selected data
          // initialize bold state.
          selections = new RBBSelection(rbbml.getRBB(MLPart.SESSION), rbbml.getRBB(MLPart.COORDINATION));
          boldSelected();
          // now make it so the bolding when be updated whenever data is selected or unselected.
          selectionChange = new RBBEventUI(250,1000) {
            @Override public void rbbEventUI(RBBEventChange[] changes) {
                boldSelected();
                constructTimeAnnotations();
            }
          };
          rbbml.getRBB(MLPart.COORDINATION).addEventListener(selectionChange, byTags("selected"));

          replayControl = new RBBReplayControl(rbbml.getRBB(MLPart.COORDINATION),
                  new RBBReplayControl.Listener() {
                        public void replayControl(long simTime, double playRate) {
                            constructTimeAnnotations();
                        }},
                        100);

        updateZLegend();

    }

    LookupPaintScale zPaintScale;
    Rectangle2D zAxisArea;
    NumberAxis zAxis;
    private PaintScaleLegend zLegend;
    
    private void updateZLegend() {
        if(!getDataset().isXYZ())
            return;

        zAxis = new NumberAxis(getDataset().getAxisString(2)) {
            { setLabelPaint(Color.BLUE); } // draw label in blue as a hint to the user that it is clickable.
            // override to save off the drawing area to detect clicks in this area later.
            @Override public AxisState draw(Graphics2D g2, double cursor, Rectangle2D plotArea, Rectangle2D dataArea, RectangleEdge edge, PlotRenderingInfo plotState) {
                zAxisArea = dataArea;
                return super.draw(g2, cursor, plotArea, dataArea, edge, plotState);
            }
        };

        Range r = getDataset().getRange(2);
        if(r==null || r.getLowerBound() == Double.NaN)
            r = new Range(0,1);
        else if(r.getLowerBound() == r.getUpperBound())
            r = new Range(r.getUpperBound()-1, r.getUpperBound());
        int gradientSteps = 10;
        zPaintScale = new LookupPaintScale(r.getLowerBound(), r.getUpperBound(), Color.RED);
        for(int i = 0; i < gradientSteps; ++i) {
            float f = (float)i/(gradientSteps-1);
            zPaintScale.add(r.getLowerBound()+(r.getUpperBound()-r.getLowerBound())*f, new Color(f, 0.0f, 1-f));
        }

        if(zLegend == null) {
            zLegend = new PaintScaleLegend(zPaintScale, zAxis);
            zLegend.setPosition(RectangleEdge.RIGHT);
            zLegend.setMargin(4, 4, 40, 4);
            zLegend.setAxisLocation(AxisLocation.BOTTOM_OR_RIGHT);
        }
        else {
            zLegend.setScale(zPaintScale);
            zAxis.setRange(r);
            zLegend.setAxis(zAxis);
        }
    }

    @Override public MLObservationsXYZDataset getDataset() { return (MLObservationsXYZDataset) super.getDataset(); }

    BasicStroke notBold = new BasicStroke(1); // thickness of line for not-selected timeseries
    BasicStroke bold = new BasicStroke(5); // thickness of line for selected timeseries
    public void boldSelected() {
        for(int i = 0; i < getDataset().getSeriesCount(); ++i) {
            getRenderer().setSeriesStroke(i, notBold); // set all not bold by default.
            getRenderer().setSeriesOutlineStroke(i, notBold);
        }
        // get the set of selected event IDs.
        Set<Long> selected = new HashSet<Long>();
        try {
            selected.addAll(Arrays.asList(selections.getSelectedEventIDs()));
        } catch (Exception ex) {
            System.err.println("MLObservationPlot: error retrieving set of selected Timeseries");
        }

        for(int i = 0; i < getDataset().getSeriesCount(); ++i) {
            if(getDataset().isInput(i, selected)) {
                getRenderer().setSeriesStroke(i, bold); // set all not bold by default.
                getRenderer().setSeriesOutlineStroke(i, bold);
            }
        }
    }

    /**
     * Return x (axis=0) or y(axis=1) axis, following
     * the axis numbering convention in MLObservationXYDataset
     */
    NumberAxis getAxis(int axis) {
        if(axis==0)
            return  (NumberAxis) getDomainAxis();
        else if(axis==1)
            return (NumberAxis) getRangeAxis();
        else if(axis==2 && getDataset().isXYZ())
            return zAxis;
        else
            return null;
    }

    int getAxisIndex(NumberAxis axisInstance) {
        for(int i = 0; i  < 3; ++i)
            if(getAxis(i) == axisInstance)
                return i;
        throw new IllegalArgumentException("MLObservationPlot.getAxisIndex paramater is not one of my axis instances!");
    }

    void selectFeatureDialog(int axis) {
        String axisName = MLObservationsXYZDataset.Axis.axisNames[axis];
        String was = getDataset().getSelectedFeatureName(axis);
        ArrayList<String> featureNames = new ArrayList<String>();
        featureNames.addAll(getDataset().seq.get(0).get(0).getMetadata().getFeatureNames());
        featureNames.add(getDataset().timeName);
        Collections.sort(featureNames);
        String selected = (String) JOptionPane.showInputDialog(null, null, "Select " + axisName + " Axis", JOptionPane.PLAIN_MESSAGE, null, featureNames.toArray(new String[0]), was);
        if(selected == null)
            return;

        int numDim = getDataset().getFeatureDimensionality(selected);

        Integer selectedDim = 0;
        if(numDim==0) {
                  JOptionPane.showMessageDialog(null, "The feature \"" + selected + "\" is not numeric data.",
				                       "Warning", JOptionPane.WARNING_MESSAGE);
        }
        else if(numDim > 1) {
            String msg = selected + " has " + numDim + " dimensions.  Please select one:";
            Integer[] dimNames = new Integer[numDim];
            for(int i = 0; i < numDim; ++i)
                dimNames[i] = i;
            selectedDim = (Integer) JOptionPane.showInputDialog(null, msg, "Select Dim", JOptionPane.PLAIN_MESSAGE, null, dimNames, "0");
            if(selectedDim == null)
                return;
        }

        getDataset().selectFeature(axis, selected, selectedDim);
        constructTimeAnnotations();

        if(axis==2) //
            updateZLegend();
        else
            getAxis(axis).setLabel(getDataset().getAxisString(axis));
    }

    void mouseClicked(ChartMouseEvent e) {
        try {
            if(e.getEntity() instanceof AxisEntity) {
                NumberAxis axisInstance = (NumberAxis) ((AxisEntity) e.getEntity()).getAxis();
                selectFeatureDialog(getAxisIndex(axisInstance));
            }
            else if(getDataset().isXYZ() && zAxisArea.contains(e.getTrigger().getPoint())) {
                selectFeatureDialog(2);
            }
            else if(e.getEntity() instanceof XYItemEntity) {
                XYItemEntity item = (XYItemEntity) e.getEntity();
                final Number x = getDataset().getX(item.getSeriesIndex(), item.getItem());
                final Number y = getDataset().getY(item.getSeriesIndex(), item.getItem());
                System.err.println("Clicked " + getDataset().getAxisString(0) + "=" + x + " " + getDataset().getAxisString(1) + "=" + y);
                Tagset selectTagset = null;
//                if(selectionTemplate.getNumTags() > 0) {
//                    selectTagset = Tagset.template(selectionTemplate, ts.getTagset());
//                    if(selectTagset.containsValue(null)) {
//                        System.err.println("Warning: the user selected a Timeseries that didn't provide all the tags for the -selectionTemplate option.\gradientSteps\tRequired:\t"+selectionTemplate+"\gradientSteps\tSelected:\t"+ts.getTagset());
//                        selectTagset = null; // fall back and select just the specific timeseries.
//                    }
//                }
                if(selectTagset != null) // if selecting by tags, clicking on one Timeseries can result in the selection of multiple related Events.
                    selections.selectEvents(byTags(selectTagset));
                else {
                    Long[] inputIDs = Event.getIDs(getDataset().seq.get(item.getSeriesIndex()).get(0).getMetadata().getAllFeatureEvents());
                    selections.selectEvents(byID(inputIDs));
                }
            }
            else {
                selections.deselectAll();

            }
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }

    private void constructTimeAnnotations() {
        try {
            final double now = rbbml.getSimTime();
            clearAnnotations();
            // get the set of selected event IDs.
            Set<Long> selectedEventIDs = new HashSet<Long>();
            try {
                selectedEventIDs.addAll(Arrays.asList(selections.getSelectedEventIDs()));
            } catch (Exception ex) {
                System.err.println("MLObservationPlot: error retrieving set of selected Timeseries");
            }

            MLObservationsXYZDataset dataset = getDataset();
            for(int i = 0; i < dataset.getSeriesCount(); ++i) {
//                MLObservationSequence seq = dataset.seq[i];
//                if(now < seq.getOldest().getTime() ||
//                        now > seq.getNewest().getTime())
//                    continue; // don't extrapolate a value outside the observations.
                final double x = dataset.interpolateValue(0, i, now);
                final double y = dataset.interpolateValue(1, i, now);
                if(Double.isNaN(x) || Double.isNaN(y))
                    continue;
                final BufferedImage img = dataset.isInput(i, selectedEventIDs) ? currentSelectedValueMarkerImage : currentValueMarkerImage;
                addAnnotation(new XYImageAnnotation(x, y, img), false);
              //  addAnnotation(new XYShapeAnnotation(new Ellipse2D.Double(, 10, 10)), false);
            }
        } catch(Exception e) {
            System.err.println("MLObservationPlot.constructTimeAnnotations error: " + e);
        }
    }

    public static ChartPanel createInPanel(RBBML rbbml, MLObservationSequence[] seq, String featureX, String featureY, String featureZ, String nameTag) throws Exception {
        final MLObservationPlot plot = new MLObservationPlot(rbbml, new MLObservationsXYZDataset(seq, featureX, featureY, featureZ, nameTag));

        //        chart.getXYPlot().addAnnotation(new XYShapeAnnotation(new Ellipse2D.Double(50, 160, 10, 10)));

        final JFreeChart chart = new JFreeChart(null, plot);

        if(plot.getDataset().isXYZ())
            chart.addSubtitle(plot.zLegend);

        // ChartUtilities.applyCurrentTheme(chart);

        chart.getLegend().setPosition(RectangleEdge.BOTTOM);

        chart.setBackgroundPaint(Color.white);

        
        
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));

        chartPanel.addChartMouseListener(new ChartMouseListener(){
            @Override
            public void chartMouseClicked(ChartMouseEvent e){
                if(e.getChart().getXYPlot() == plot)
                    plot.mouseClicked(e);
            }

            public void chartMouseMoved(ChartMouseEvent event) {
            }
        });

        return chartPanel;
    }

}
         


