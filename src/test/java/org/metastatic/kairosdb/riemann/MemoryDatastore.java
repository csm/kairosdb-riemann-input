package org.metastatic.kairosdb.riemann;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.SetMultimap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;

import java.io.IOException;
import java.util.*;

/**
 * Fake datastore.
 */
public class MemoryDatastore implements Datastore {
    public class Entry implements Comparable<Entry> {
        public Entry(DataPoint dataPoint, ImmutableSortedMap<String, String> tags) {
            this.dataPoint = dataPoint;
            this.tags = tags;
        }

        final DataPoint dataPoint;
        final ImmutableSortedMap<String, String> tags;

        public int compareTo(Entry o) {
            return Long.valueOf(this.dataPoint.getTimestamp()).compareTo(o.dataPoint.getTimestamp());
        }
    }
    public final Map<String, LinkedList<Entry>> dataPoints = new HashMap<String, LinkedList<Entry>>();

    public void close() throws InterruptedException, DatastoreException {
    }

    public void putDataPoint(String s, ImmutableSortedMap<String, String> immutableSortedMap, DataPoint dataPoint) throws DatastoreException {
        LinkedList<Entry> list = dataPoints.get(s);
        if (list == null) {
            list = new LinkedList<Entry>();
            dataPoints.put(s, list);
        }
        list.addLast(new Entry(dataPoint, immutableSortedMap));
    }

    public Iterable<String> getMetricNames() throws DatastoreException {
        return dataPoints.keySet();
    }

    public Iterable<String> getTagNames() throws DatastoreException {
        Set<String> tagNames = new TreeSet<String>();
        for (LinkedList<Entry> e : dataPoints.values()) {
            for (Entry ee : e) {
                tagNames.addAll(ee.tags.keySet());
            }
        }
        return tagNames;
    }

    public Iterable<String> getTagValues() throws DatastoreException {
        Set<String> tagValues = new TreeSet<String>();
        for (LinkedList<Entry> e : dataPoints.values()) {
            for (Entry ee : e) {
                tagValues.addAll(ee.tags.keySet());
            }
        }
        return tagValues;
    }

    private boolean matchTags(ImmutableSortedMap<String, String> tags, SetMultimap<String, String> query) {
        for (Map.Entry<String, String> q : query.entries()) {
            if (q.getValue().equals(tags.get(q.getKey()))) {
                return true;
            }
        }
        return false;
    }

    private boolean limit(int limit, int query) {
        return query > 0 && limit < query;
    }

    public void queryDatabase(DatastoreMetricQuery datastoreMetricQuery, QueryCallback queryCallback) throws DatastoreException {
        try {
            LinkedList<Entry> list = dataPoints.get(datastoreMetricQuery.getName());
            NavigableSet<Entry> entries = new TreeSet<Entry>();
            if (list != null) {
                for (Entry e : list) {
                    if (e.dataPoint.getTimestamp() >= datastoreMetricQuery.getStartTime()
                            && e.dataPoint.getTimestamp() <= datastoreMetricQuery.getEndTime()
                            && matchTags(e.tags, datastoreMetricQuery.getTags())
                            && limit(entries.size(), datastoreMetricQuery.getLimit())) {
                        entries.add(e);
                    }
                }
            }
            Iterator<Entry> it = null;
            switch (datastoreMetricQuery.getOrder()) {
                case ASC:
                    it = entries.iterator();
                    break;
                case DESC:
                    it = entries.descendingIterator();
                    break;
            }
            while (it.hasNext()) {
                queryCallback.addDataPoint(it.next().dataPoint);
            }
            queryCallback.endDataPoints();
        } catch (IOException e) {
            throw new DatastoreException(e);
        }
    }

    public void deleteDataPoints(DatastoreMetricQuery datastoreMetricQuery) throws DatastoreException {
        throw new UnsupportedOperationException();
    }

    public TagSet queryMetricTags(DatastoreMetricQuery datastoreMetricQuery) throws DatastoreException {
        throw new UnsupportedOperationException();
    }
}
