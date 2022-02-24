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

    public HeaderHierarchy() {
        this(null, -1, null);
    }

    public HeaderHierarchy(String name, int index, HeaderHierarchy parent) {
        this.name = name;
        this.index = index;
        this.children = new HashMap<>();
        this.parent = parent;
    }

    public void add(int i, List<String> item) {
        Objects.requireNonNull(item);
        if (item.size() == 0) return;
        HeaderHierarchy child = this.children.computeIfAbsent(item.get(0),
                k -> new HeaderHierarchy(k, item.size() == 1 ? i : -1, this));
        child.add(i, item.subList(1, item.size()));
    }

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
