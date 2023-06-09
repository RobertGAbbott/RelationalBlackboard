/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ui;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventChange.Added;
import gov.sandia.rbb.RBBEventChange.DataAdded;
import gov.sandia.rbb.RBBEventChange.Modified;
import gov.sandia.rbb.RBBEventChange.Removed;
import gov.sandia.rbb.RBBEventListener;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.PreparedStatementCache;
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.impl.h2.statics.H2STagset;
import java.awt.Component;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.imageio.ImageIO;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.ExpandVetoException;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

/**
 *
 *
 * Manual Test Procedure:
 *
 *
 * @author rgabbot
 */
public class TagTree extends JTable implements TreeWillExpandListener {

    JTree tree;
    RBB rbb;
    Integer totalNumEvents;
    String timeCoordinate;

    abstract class TagsetNode extends DefaultMutableTreeNode implements RBBEventListener {
        String label, labelPrefix="",labelPostfix="";

        /*
         * number of immedediate descendents in the tree.
         * For a Name node, this is all the values found for my unbound tag name.
         * For a value node, this is all the different tag names attached to Events that match my tagset, excluding tag names that are in my tagset.
         *
         */
        int numChildren;

        /*
         * Number of events in the RBB that match my tagset.
         */
        int numDescendants;

        TagsetNode(String label, int numEvents) {
            // If my tagset has no null values, my children are the names/counts of all the distinct tag names in tagsets that are supersets of me.
            // if I have a null value, my children are the different values of that null-valued tags
            this.label= label;
            this.numDescendants = numEvents;
        }
        
        abstract int makeChildren(int max) throws SQLException;

        /**
         * If not overridden,will use default icon.
         */
        Icon getIcon() { return null; };

        @Override
        public boolean isLeaf() {
            return false; // we don't yet know how many children it will have, but it does have some.
        }

        /*
         * Get the tagset representing the path from the root node to this node.
         * Note the tags will be in the order encountered rather than in standard/sorted order,
         * Because toString() displays this and so it should reflect the path the user
         * took to this node.
         */
        String getTagset() {
            StringBuilder sb = new StringBuilder();
            for(TreeNode tn : getPath()) {
                if(tn instanceof NameNode) {
                    if(sb.length() > 0)
                        sb.append(",");
                    sb.append(((NameNode)tn).label);
                }
                else if(tn instanceof ValueNode)
                    sb.append("=" + ((ValueNode)tn).label);
            }

            return sb.toString();
        }

        @Override
        public String toString() {
            return getTagset() + " ("+numDescendants+" Events, "+(numDescendants*1000/totalNumEvents)/10.0+"%)";
        }

        @Override
        public void eventModified(RBB rbb, Modified ec) {
            for(TagsetNode tn : getMatchingChildren(ec.event.getTagset()))
                tn.eventModified(rbb ,ec);
        }

        @Override
        public void eventDataAdded(RBB rbb, DataAdded ec) {
            // don't care... not displaying timeseries information.
        }

        boolean anyChildMatchesName(String name) {
            for(int i = 0; i < getChildCount(); ++i)
                if(getChildAt(i) instanceof NameNode && ((NameNode)getChildAt(i)).label.equals(name))
                    return true;
            return false;
        }


        /*
         * Updates the stats on descendants and propagates to children.
         * Overrides should call this, then add a new child node if they are
         * currently expanded
         */
        @Override
        public void eventAdded(RBB rbb, Added ec) {
            ++numChildren;
            ++numDescendants;
            getTreeModel().nodeChanged(this);

            for(TagsetNode tn : getMatchingChildren(ec.event.getTagset())) {
                tn.eventAdded(rbb,ec);
            }
        }

        @Override
        public void eventRemoved(RBB rbb, Removed ec) {
            --numChildren;
            --numDescendants;
            getTreeModel().nodeChanged(this);

            // Tagset tags = ec.event.getTagset();

            for(TagsetNode tn : getMatchingChildren(ec.event.getTagset())) {
                tn.eventRemoved(rbb ,ec);
                if(tn.numDescendants==0)
                    getTreeModel().removeNodeFromParent(tn);
            }
        }

        /*
         * The base class implementation fails because it doesn't go through the model.
         */
        @Override
        public void removeAllChildren() {
            while(getChildCount() >0)
                getTreeModel().removeNodeFromParent((MutableTreeNode)getChildAt(0));
        }

        /**
         * No other functions should be used to add nodes.
         *
         * In particular, mixing DefaultMutableTreeNode.add() with DefaultTreeModel.insertNodeInto() causes the tree
         * model to crash with an indexoutofboundsexception.  (Perhaps add() doesn't update the model).
         */
        protected void appendNode(MutableTreeNode n) {
            getTreeModel().insertNodeInto(n, this, getChildCount());
        }

        protected ArrayList<TagsetNode> getMatchingChildren(Tagset tags) {
            ArrayList<TagsetNode> result = new ArrayList<TagsetNode>();
            for(int i = 0; i < getChildCount(); ++i) {
                if(!(getChildAt(i) instanceof TagsetNode))
                    continue;
                TagsetNode child = (TagsetNode) getChildAt(i);

                if(child instanceof NameNode && !tags.containsName(child.label))
                    continue;
                
                if(child instanceof ValueNode && !tags.contains(label, child.label))
                    continue;

                result.add(child);
            }
            
            return result;
        }

        protected boolean isExpanded() {
            return tree.isExpanded(new TreePath(this.getPath()));
        }

    }

    class RootNode extends TagsetNode {
        RootNode(int numEvents) {
            super("", numEvents);
        }
        @Override
        int makeChildren(int max) throws SQLException {
            removeAllChildren();
            appendNode(new EventsNode(totalNumEvents));
            
            ResultSet rs = rbb.db().createStatement().executeQuery("select rbb_id_to_string(NAME_ID) NAME, N from (select name_id, count(name_id) N  from rbb_tagsets T join rbb_events E on T.tagset_id = E.tagset_id group by NAME_ID) order by N desc, NAME;");
            int childrenFound = 0;
            while(rs.next()) {
                appendNode(new NameNode(rs.getString("NAME"), rs.getInt("N")));
                ++childrenFound;
            }
            return childrenFound;
        }

        @Override
        public String toString() {
            return totalNumEvents + " Total Events";
        }

        @Override
        public void eventAdded(RBB rbb, Added ec) {
            super.eventAdded(rbb,ec);
            ++totalNumEvents;

            if(!isExpanded())
                return;

            for(String name : ec.event.getTagset().getNames()) {
                if(!anyChildMatchesName(name))
                    appendNode(new NameNode(name, 1));
            }
        }

        @Override
        public void eventRemoved(RBB rbb, Removed ec) {
            --totalNumEvents; // the root node, alone, keeps track of total event count.
            super.eventRemoved(rbb, ec);
        }


    }

    /*
     * A NameNode supplies a name for a tagset, but no value for it.
     */
    class NameNode extends TagsetNode {

        NameNode(String label, int numEvents) {
            super(label, numEvents);
            labelPostfix="=";
        // DefaultMutableTreeNode(totalNumEvents + " Events in " + args[0]);
        }

        @Override
        int makeChildren(int maxChildren) throws SQLException {
            removeAllChildren();
            appendNode(new EventsNode(numDescendants));
            ResultSet rs = H2STagset.findCombinations(rbb.db(), label, getTagset().toString(), "RBB_EVENTS", "TAGSET_ID");
            int numFound=0;
            while(rs.next()) {
                Tagset tc = new Tagset(rs.getString("TAGS"));
                int n = rs.getInt("N");
                appendNode(new ValueNode(tc.getValue(label).toString(), n));
                if(++numFound==maxChildren)
                    break;
            }
            rs.close();
            return numFound;
        }

        @Override
        Icon getIcon() {
            return TagNodeRenderer.tagNameIcon;
        }

        @Override
        public void eventAdded(RBB rbb, Added ec) {
            super.eventAdded(rbb,ec);

            if(!isExpanded())
                return;

            Tagset tags = ec.event.getTagset();
            if(!anyChildMatchesValue(tags.getValue(label))) {
                appendNode(new ValueNode(tags.getValue(label), 1));
            }
        }

        boolean anyChildMatchesValue(String value) {
            for(int i = 0; i < getChildCount(); ++i)
                if(getChildAt(i) instanceof ValueNode && ((ValueNode)getChildAt(i)).label.equals(value))
                    return true;
            return false;
        }
    
    
    }


    class ValueNode extends TagsetNode {
        ValueNode(String label, int numEvents) {
            super(label, numEvents);
            labelPrefix="=";
        }
        @Override
        int makeChildren(int maxChildren) throws SQLException {
            removeAllChildren();
            appendNode(new EventsNode(numDescendants));

            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(rbb.db());
            q.add("select rbb_id_to_string(name_id) NAME, name_id, count(*) N from(select name_id from rbb_tagsets T join rbb_events E on T.TAGSET_ID=E.TAGSET_ID where T.tagset_id in (");
            H2STagset.hasTagsQuery(rbb.db(), getTagset(), q);
            q.add(")) group by NAME_ID order by N desc, NAME limit "+maxChildren);

            // System.err.println(q);
            ResultSet rs = q.getPreparedStatement().executeQuery();
            // don't list tag names already in this path
            int numFound=0;
            while(rs.next()) {
                final String name = rs.getString("NAME");
                if(anyAncestorMatchesName(name))
                    continue;
                final int numEvents = rs.getInt("N");
                appendNode(new NameNode(name, numEvents));
                if(++numFound==maxChildren)
                    break;
            }
            rs.close();
            return numFound;
        }

        @Override
        Icon getIcon() {
            return TagNodeRenderer.tagValueIcon;
        }

        @Override
        public void eventAdded(RBB rbb, Added ec) {
            super.eventAdded(rbb,ec);

            if(!isExpanded())
                return;

            for(String name : ec.event.getTagset().getNames()) {
                if(!anyAncestorMatchesName(name) && !anyChildMatchesName(name))
                    appendNode(new NameNode(name, 1));
            }
        }

        boolean anyAncestorMatchesName(String name) {
            for(TreeNode tn : this.getPath())
                if(tn instanceof NameNode)
                    if(((NameNode)tn).label.equals(name))
                        return true;
            return false;
        }

    }

    class EventsNode extends TagsetNode {
        EventsNode(int numEvents) {
            super(null,numEvents);
        }
        @Override
        int makeChildren(int maxChildren) throws SQLException {
            removeAllChildren();
            int numFound=0;
            for(Event e : Event.find(rbb.db(), byTags(getTagset()), withTimeCoordinate(timeCoordinate))) {
                appendNode(new DefaultMutableTreeNode(e));
                if(++numFound==maxChildren)
                    break;
            }
            return numFound;
        }
        @Override
        public String toString() {
            return "Events ("+super.numDescendants+")";
        }

        @Override
        Icon getIcon() {
            return TagNodeRenderer.eventsIcon;
        }

        @Override
        public void eventAdded(RBB rbb, Added ec) {
            super.eventAdded(rbb,ec);

            if(!isExpanded())
                return;

            appendNode(new DefaultMutableTreeNode(ec.event));
        }

        @Override
        public void eventRemoved(RBB rbb, Removed ec) {
            super.eventRemoved(rbb,ec);

            MutableTreeNode child = findChildForEvent(ec.event.getID());
            if(child != null)
                getTreeModel().removeNodeFromParent(child);
        }

        /*
         * returns null if no child matches the RBB Event ID
         */
        private MutableTreeNode findChildForEvent(Long eventID) {
            for(int i = 0; i < getChildCount(); ++i) {
                DefaultMutableTreeNode child = (DefaultMutableTreeNode) getChildAt(i);
                Object ob = child.getUserObject();
                if(!(ob instanceof Event))
                    continue;

                Event ev = (Event)child.getUserObject();
                if(ev.getID().equals(eventID))
                    return child;
            }

            return null;
        }


        @Override
        public void eventModified(RBB rbb, Modified ec) {
            MutableTreeNode child = findChildForEvent(ec.event.getID());
            if(child==null)
                return; // this "shouldn't" ever happen...

            child.setUserObject(ec.event);
            getTreeModel().nodeChanged(child);
        }

    }

    private static class TagNodeRenderer extends DefaultTreeCellRenderer {

        private static Icon tagNameIcon=getIcon("TagNameIcon.png"), tagValueIcon=getIcon("TagValueIcon.png"), eventsIcon=getIcon("EventsIcon.png");

        private static Icon getIcon(String name) {
            try {
                return new ImageIcon(ImageIO.read(TagNodeRenderer.class.getResourceAsStream("/gov/sandia/rbb/ui/images/" + name)));
            } catch (Exception ex) {
                System.err.println("Error loading icon "+name);
                return null;
            }
        }

        @Override
        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean exp, boolean leaf, int row, boolean hasFocus) {
            if (value instanceof TagsetNode) {
                Icon icon = ((TagsetNode)value).getIcon();
                if(icon == null)
                    icon = getDefaultOpenIcon();
                setOpenIcon(icon);
                setClosedIcon(icon);
            } else {
                setOpenIcon(getDefaultOpenIcon());
                setClosedIcon(getDefaultClosedIcon());
            }
            super.getTreeCellRendererComponent(tree, value, sel, exp, leaf, row, hasFocus);
            return this;
        }
    }

    /*
     * Construct / parse args, then initiate event processing from Swing thread.
     */
    TagTree(RBB rbb_, String... args) throws Exception {
        this.rbb = rbb_;

        int iArgs = 0;
        for(; iArgs < args.length && args[iArgs].startsWith("-"); ++iArgs) {
            if(args[iArgs].equalsIgnoreCase("-timeCoordinate")) {
               if(++iArgs >= args.length)
                    throw new Exception("-timeCoordinate requires a tagset argument.  Try running with -help.");
                this.timeCoordinate = args[iArgs];
            }
            else {
                throw new Exception("Unrecognized arg " + args[iArgs] + ".  Try running with -help.");
            }
        }

        Statement s = rbb.db().createStatement();
        ResultSet rs = s.executeQuery("select count(*) from RBB_EVENTS");
        rs.next();
        totalNumEvents = rs.getInt(1);

        RootNode root = new RootNode(1);
        
        tree = new JTree(root);
        tree.addTreeWillExpandListener(this);
        tree.setCellRenderer(new TagNodeRenderer());

        JScrollPane treeView = new JScrollPane(tree);
        frame = new JFrame();
        frame.setSize(350, 700);
        frame.add(treeView);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        tree.addTreeSelectionListener(new TreeSelectionListener() {
            @Override
            public void valueChanged(TreeSelectionEvent e) {
                if((e.getPath().getLastPathComponent() instanceof TagsetNode)) {
                    TagsetNode tn = (TagsetNode) e.getPath().getLastPathComponent();
                    frame.setTitle(tn.getTagset());
                }
                else {
                    frame.setTitle(e.getPath().getLastPathComponent().toString());
                }
            }
        });
                    // RBB rbb, Tagset tags, Listener listener, int laziness, Integer impatience

        root.makeChildren(-1);

        rbb.addEventListener(new RBBEventUI(100, 250) {
            @Override public void rbbEventUI(RBBEventChange[] changes) {
                for(RBBEventChange ec : changes) {
                    RootNode root = (RootNode) getTreeModel().getRoot();
                    ec.dispatch(rbb, root);
                }
            }
        }, new RBBFilter());

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                try {
                    frame.setVisible(true);
                } catch(Exception e) {
                    System.err.println(e);
                }
            }
        });

    }

    JFrame frame;

    @Override
    public void treeWillExpand(TreeExpansionEvent event) throws ExpandVetoException {
        Object opened = event.getPath().getLastPathComponent();
        if(opened instanceof TagsetNode) {
            final int maxChildren=5000;
            try {
                frame.setCursor(java.awt.Cursor.getPredefinedCursor(java.awt.Cursor.WAIT_CURSOR));
                TagsetNode tn = (TagsetNode) opened;
                int numChildren = tn.makeChildren(maxChildren);
                frame.setCursor(java.awt.Cursor.getPredefinedCursor(java.awt.Cursor.DEFAULT_CURSOR));
                if(numChildren >= maxChildren)
                    JOptionPane.showMessageDialog(null, "Warning: results limited to "+maxChildren, "Display Limit Reached", JOptionPane.INFORMATION_MESSAGE);
            } catch (SQLException ex) {
                System.err.println(ex);
            }
        }
    }

    public DefaultTreeModel getTreeModel() {
        return (DefaultTreeModel) tree.getModel();
    }
        
    @Override
    public void treeWillCollapse(TreeExpansionEvent event) throws ExpandVetoException {
        // note that DefaultMutableTreeNode.removeAllChildren() does not work, perhaps
        // because it does not notify the TreeModel
        Object opened = event.getPath().getLastPathComponent();
        if(opened instanceof TagsetNode) {
            TagsetNode tn = (TagsetNode) opened;
            tn.removeAllChildren();

            // This dummy node is added because otherwise the arrow that shows this node
            // can be opened disappears when its children are removed.
            // The dummy will be removed by removeAllChildren.
            tn.appendNode(new DefaultMutableTreeNode());
        }
    }
    
}
