package org.radarbase.mock.data

import java.util.*

/**
 * Header hierarchy child node. Usually accessed via [.add]
 *
 * @param name name of the node
 * @param index index in the csv file. -1 if not a leaf node.
 * @param parent parent node.
 */
class HeaderHierarchy(
    val name: String? = null,
    index: Int = -1,
    private val parent: HeaderHierarchy? = null
) {
    /** The index of current element. */
    val index: Int = index
        get() {
            check(field != -1) { "Header does not exist" }
            return field
        }

    private val _children: MutableMap<String, HeaderHierarchy> = HashMap()
    val children: Map<String, HeaderHierarchy>
        get() = _children.toMap()

    /**
     * Add child nodes to this node. Each item in the list will become a new level down, and the
     * last item will become a leaf node with given index.
     *
     * @param index index of the item.
     * @param item list of item elements, each one level deeper than the previous.
     */
    fun add(index: Int, item: List<String>) {
        Objects.requireNonNull(item)
        if (item.isEmpty()) {
            return
        }
        val child = _children.computeIfAbsent(item[0]) { k ->
            HeaderHierarchy(k, if (item.size == 1) index else -1, this)
        }
        child.add(index, item.subList(1, item.size))
    }

    private fun StringBuilder.appendHeader() {
        parent?.run {
            appendHeader()
        }
        if (name != null) {
            append('.')
            append(name)
        }
    }

    override fun toString(): String = buildString(50) {
        appendHeader()
        if (index >= 0) {
            append('[')
            append(index)
            append(']')
        }
    }
}
