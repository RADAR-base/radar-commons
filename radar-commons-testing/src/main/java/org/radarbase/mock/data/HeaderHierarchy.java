package org.radarbase.mock.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HeaderHierarchy {
    private final int index;
    private final Map<String, HeaderHierarchy> children;
    private final HeaderHierarchy parent;
    private final String name;

    /** Root node. */
    public HeaderHierarchy() {
        this(null, -1, null);
    }

    /**
     * Header hierarchy child node. Usually accessed via {@link #add(int, List)}
     *
     * @param name name of the node
     * @param index index in the csv file. -1 if not a leaf node.
     * @param parent parent node.
     */
    public HeaderHierarchy(String name, int index, HeaderHierarchy parent) {
        this.name = name;
        this.index = index;
        this.children = new HashMap<>();
        this.parent = parent;
    }

    /**
     * Add child nodes to this node. Each item in the list will become a new level down, and the
     * last item will become a leaf node with given index.
     *
     * @param i index if the item.
     * @param item list of item elements, each one level deeper than the previous.
     */
    public void add(int i, List<String> item) {
        Objects.requireNonNull(item);
        if (item.isEmpty()) {
            return;
        }
        HeaderHierarchy child = this.children.computeIfAbsent(item.get(0),
                k -> new HeaderHierarchy(k, item.size() == 1 ? i : -1, this));
        child.add(i, item.subList(1, item.size()));
    }

    /**
     * Get the index of current element.
     *
     * @return index
     * @throws IllegalStateException if current node is not a leaf.
     */
    public int getIndex() {
        if (index == -1) {
            throw new IllegalStateException("Header does not exist");
        }
        return index;
    }

    public Map<String, HeaderHierarchy> getChildren() {
        return children;
    }

    private void appendTo(StringBuilder builder) {
        if (parent != null) {
            parent.appendTo(builder);
        }
        if (name != null) {
            builder.append('.').append(name);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(50);
        appendTo(builder);
        if (index >= 0) {
            builder.append('[')
                    .append(index)
                    .append(']');
        }
        return builder.toString();
    }

    public String getName() {
        return name;
    }
}
