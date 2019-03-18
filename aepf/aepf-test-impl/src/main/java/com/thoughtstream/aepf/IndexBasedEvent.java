package com.thoughtstream.aepf;

import com.thoughtstream.aepf.beans.Event;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public class IndexBasedEvent implements Event {
    private final int index;

    public IndexBasedEvent(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexBasedEvent that = (IndexBasedEvent) o;

        return index == that.index;

    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public String toString() {
        return "IndexBasedEvent{" +
                "index=" + index +
                '}';
    }
}
