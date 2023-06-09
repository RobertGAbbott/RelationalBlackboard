/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ui;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange.Added;
import gov.sandia.rbb.RBBEventChange.DataAdded;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.tools.SortedTagsets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Arrays;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class TagTable extends JTable {

    public static class TagTableModel extends AbstractTableModel {

        SortedTagsets tagsets;
        RBBEventUI changeListener;

        public static final String usage =
                "Usage: getTagsets <RBBURL> [UI Options] [Sort Options] tagName1[,tagName2,...]\n"+
                "UI Options:\n"+
                "\t-dynamicFilterTags <tagname1,tagname2>: In addition to filterTags, plot only timeseries with these tag names, and the values for these tags taken from the most recent Event with a tag named filterTags.\n"+
                "Sort Options:\n"+
                SortedTagsets.recognizedArgs;
        
        public TagTableModel(RBB rbb, String... args) throws Exception {
            String[] dynamicFilterTags=null;
            tagsets = new SortedTagsets(rbb);

            // arg parsing is divided between this class and SortedTagsets.
            try {
                int iArgs = 0;
                for(; iArgs < args.length; ++iArgs) {
                    if(args[iArgs].equalsIgnoreCase("-dynamicFilterTags"))
                    {
                       if(++iArgs >= args.length)
                            throw new Exception("-dynamicFilterTags requires a comma-separated list of tag names.");
                       dynamicFilterTags = args[iArgs].split(",");
                    }
                    else {
                        break; // after the UI options come the Sort Options
                    }
                }
                tagsets.parseArgs(Arrays.copyOfRange(args, iArgs, args.length));
            }
            catch(Exception e) {
                throw new Exception("TagTable error parsing args: "+e.getMessage()+"\n"+usage);
            }

            RBBEventUI changeListener = new RBBEventUI(250,2000) {
                @Override public void rbbEventUI(RBBEventChange[] changes) {
                    try {
                        tagsets.invalidate(); // access/modify tagsets only from GUI thread.
                        fireTableDataChanged();
                    } catch (Exception ex) {
                    }
                }
            };
            rbb.addEventListener(changeListener, byTags(tagsets.getFilterTags()));

            if(dynamicFilterTags != null)
                startListeningForFilterTags(rbb, dynamicFilterTags);
        }


        @Override
        public int getColumnCount() {
            return tagsets.getNumTags();
        }

        @Override
        public int getRowCount() {
            try {
                return tagsets.getNumTagsets();
            } catch (Exception ex) {
                System.err.println(ex);
                return 0;
            }
        }
        
        @Override
        public Object getValueAt(int row, int col) {
            try {
                return tagsets.getTagValue(row, col);
            } catch(Exception ex) {
                return null;
            }
        }

        @Override
        public String getColumnName(int column) {
            return tagsets.getTagName(column);
        }

        private void startListeningForFilterTags(RBB rbb, final String[] dynamicFilterTags) throws Exception {
            RBBEventUI changedFilterTags = new RBBEventUI(0,null) {
                @Override public void rbbEventUI(RBBEventChange[] changes) {
                    for(int i = changes.length-1; i >=0; --i) { // use the last filterTags Event that was Added.
                        if(!(changes[i] instanceof RBBEventChange.Added))
                            continue;
                        applyFilterTags(new Tagset(changes[i].event.getTagset().getValue("filterTags")));
                        return;
                    }
                }
                private void applyFilterTags(Tagset filterTagsSent) {
                    Tagset newFilterTags = new Tagset(tagsets.getFilterTags());
                    for(String tagName : dynamicFilterTags)
                        newFilterTags.set(tagName, filterTagsSent.getValue(tagName));
                    tagsets.setFilterTags(newFilterTags.toString()); // access/modify tagsets only from GUI thread.
                    fireTableDataChanged();
                }
            };

            rbb.addEventListener(changedFilterTags, byTags("filterTags"));          
        }
    } // end of TagTableModel


    public TagTable(RBB rbb, String... args) throws Exception {
        super(new TagTableModel(rbb, args));
        getTableHeader().addMouseListener(new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent mouseEvent) {
            int index = convertColumnIndexToModel(columnAtPoint(mouseEvent.getPoint()));
            if (index >= 0)
              System.out.println("Clicked on column " + index);
          }
        });
    }

    /**
     *
     * Direct call of TagTableMain.
     * Normally you would call RBB ui main tagtable instead but this is convenient mainly for testing.
     *
     */
    public static void main(String... args) {
        try {
            if(args.length==0)
                throw new Exception("TagTable exception: first arg is RBB JDBC URL");
            RBB rbb = RBB.connect(args[0]);
            TagTableMain(rbb, Arrays.copyOfRange(args, 1, args.length));
        } catch(Throwable ex) {
            System.err.println(ex.getMessage());
            System.exit(-1);
        }
    }

    /**
     * unlike main(), TagTableMain() propagates exceptions and allows using an already-open RBB
     * so it is more useful for calling from other code or unit tests.
     *
     * To call it from other code you will typically:
     * import static gov.sandia.rbb.ui.TagTableMain
     *
     */
    public static void TagTableMain(RBB rbb, String... args) throws Throwable {
        JTable table = new TagTable(rbb, args);
        JScrollPane scrollpane = new JScrollPane(table);
        JFrame frame = new JFrame();
        frame.setSize(350, 700);
        frame.add(scrollpane);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }

    public static class WhenSettled {

        private int delay, maxDelay;

        public WhenSettled(int delay, int maxDelay) {
            this.delay = delay;
            this.maxDelay = maxDelay;
        }

        public void poke() {

        }

        public void fire() {
            System.err.println("fire");
        }
    }

}
