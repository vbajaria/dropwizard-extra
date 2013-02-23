package com.datasift.dropwizard.hbase.scanner;

import com.datasift.dropwizard.hbase.InstrumentedHBaseClient;
import com.datasift.dropwizard.hbase.metrics.HBaseInstrumentation;
import com.datasift.dropwizard.hbase.util.TimerStoppingCallback;
import com.google.common.base.Charsets;
import com.stumbleupon.async.Deferred;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.TimerContext;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * A {@link RowScanner} that is instrumented with {@link Metric}s.
 *
 * To obtain an instance of a {@link RowScanner}, call
 * {@link InstrumentedHBaseClient#scan(byte[])}.
 */
public class InstrumentedRowScanner implements RowScanner {

    private final RowScanner scanner;
    private final HBaseInstrumentation metrics;

    /**
     * Creates a new {@link InstrumentedRowScanner} for the given underlying
     * {@link RowScanner}, instrumented using the given
     * {@link HBaseInstrumentation}.
     *
     * @param scanner the underlying {@link RowScanner} implementation
     * @param metrics the {@link Metric}s to instrument this
     *                {@link InstrumentedRowScanner} with
     */
    public InstrumentedRowScanner(final RowScanner scanner,
                                  final HBaseInstrumentation metrics) {
        this.scanner = scanner;
        this.metrics = metrics;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @see RowScanner#setStartKey(byte[])
     * @param key the first key to scan from (inclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStartKey(final byte[] key) {
        scanner.setStartKey(key);
        return this;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @see RowScanner#setStartKey(String)
     * @param key the first key to scan from (inclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStartKey(final String key) {
        scanner.setStartKey(key);
        return this;
    }

    /**
     * Set the end key in the range to scan.
     *
     * @see RowScanner#setStopKey(byte[])
     * @param key the end key to scan until (exclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStopKey(final byte[] key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the end key in the range to scan.
     *
     * @see RowScanner#setStopKey(byte[])
     * @param key the end key to scan until (exclusive)
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setStopKey(final String key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @see RowScanner#setFamily(byte[])
     * @param family the family to scan
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setFamily(final byte[] family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @see RowScanner#setFamily(String)
     * @param family the family to scan
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setFamily(final String family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @see RowScanner#setQualifier(byte[])
     * @param qualifier the family to select from cells
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setQualifier(final byte[] qualifier) {
        scanner.setQualifier(qualifier);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @see RowScanner#setQualifier(String)
     * @param qualifier the family to select from cells
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setQualifier(final String qualifier) {
        scanner.setQualifier(qualifier);
        return this;
    }

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see RowScanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setKeyRegexp(final String regexp) {
        scanner.setKeyRegexp(regexp);
        return this;
    }

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @see RowScanner#setKeyRegexp(String)
     * @param regexp a regular expression to filter keys with
     * @param charset the charset to decode the keys as
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setKeyRegexp(final String regexp, final Charset charset) {
        scanner.setKeyRegexp(regexp, charset);
        return this;
    }

    public RowScanner setKeyRegexp(byte[] regexp, Charset charset) {
        scanner.setKeyRegexp(regexp, charset);
        return this;
    }

    public RowScanner setKeyRegexp(byte[] regexp) {
        return this.setKeyRegexp(regexp, Charsets.ISO_8859_1);
    }

    public byte[] getKeyRegexp(byte[] regexp, Charset charset) {
        return scanner.getKeyRegexp(regexp, charset);
    }

    public byte[] getKeyRegexp(byte[] regexp) {
        return this.getKeyRegexp(regexp, Charsets.ISO_8859_1);
    }

    public byte[] getKeyRegexp(String regexp, Charset charset) {
        return this.getKeyRegexp(Bytes.UTF8(regexp), Charsets.ISO_8859_1);
    }

    public RowScanner setColumnRange(byte[] minColumn, byte[] maxColumn) {
        return this.setColumnRange(minColumn, true, maxColumn, true);
    }

    public RowScanner setColumnRange(byte[] minColumn, boolean minColumnInclusive, byte[] maxColumn, boolean maxColumnInclusive) {
        scanner.setColumnRange(minColumn, minColumnInclusive, maxColumn, maxColumnInclusive);
        return this;
    }

    public byte[] getColumnRange(byte[] minColumn, byte[] maxColumn) {
        return this.getColumnRange(minColumn, true, maxColumn, true);
    }

    public byte[] getColumnRange(byte[] minColumn, boolean minColumnInclusive, byte[] maxColumn, boolean maxColumnInclusive) {
        return scanner.getColumnRange(minColumn, minColumnInclusive, maxColumn, maxColumnInclusive);
    }

    public RowScanner setFilterList(byte[]... filters) {
        scanner.setFilterList(filters);
        return this;
    }

    public byte[] getPrefix(final String prefix) {
        return getPrefix(prefix.getBytes());
    }

    public byte[] getPrefix(final byte[] prefix) {
        return scanner.getPrefix(prefix);
    }

    public byte[] getColumnPrefix(String prefix) {
        return getColumnPrefix(prefix.getBytes());
    }

    public byte[] getColumnPrefix(byte[] prefix) {
        return scanner.getColumnPrefix(prefix);
    }

    public RowScanner setPrefix(final String prefix) {
        return setPrefix(prefix.getBytes());
    }

    public RowScanner setPrefix(final byte[] prefix) {
        scanner.setPrefix(prefix);
        return this;
    }

    public RowScanner setColumnPrefix(final String prefix) {
        return setColumnPrefix(prefix.getBytes());
    }

    public RowScanner setColumnPrefix(final byte[] prefix) {
        scanner.setColumnPrefix(prefix);
        return this;
    }

    /**
     * Set whether to use the server-side block cache during the scan.
     *
     * @see RowScanner#setServerBlockCache(boolean)
     * @param populateBlockcache whether to use the server-side block cache
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setServerBlockCache(final boolean populateBlockcache) {
        scanner.setServerBlockCache(populateBlockcache);
        return this;
    }

    /**
     * Set the maximum number of rows to fetch in each batch.
     *
     * @see RowScanner#setMaxNumRows(int)
     * @param maxRows the maximum number of rows to fetch in each batch
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxNumRows(final int maxRows) {
        scanner.setMaxNumRows(maxRows);
        return this;
    }

    /**
     * Set the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @see RowScanner#setMaxNumKeyValues(int)
     * @param maxKeyValues the maximum number of {@link KeyValue}s to fetch in
     *                    each batch
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxNumKeyValues(final int maxKeyValues) {
        scanner.setMaxNumKeyValues(maxKeyValues);
        return this;
    }

    /**
     * Sets the minimum timestamp of the cells to yield.
     *
     * @see RowScanner#setMinTimestamp(long)
     * @param timestamp the minimum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMinTimestamp(final long timestamp) {
        scanner.setMinTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the minimum timestamp of the cells to yield.
     *
     * @see RowScanner#getMinTimestamp()
     * @return the minimum timestamp of the cells to yield
     */
    public long getMinTimestamp() {
        return scanner.getMinTimestamp();
    }

    /**
     * Sets the maximum timestamp of the cells to yield.
     *
     * @see RowScanner#setMaxTimestamp(long)
     * @param timestamp the maximum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setMaxTimestamp(final long timestamp) {
        scanner.setMaxTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the maximum timestamp of the cells to yield.
     *
     * @see RowScanner#getMaxTimestamp()
     * @return the maximum timestamp of the cells to yield
     */
    public long getMaxTimestamp() {
        return scanner.getMaxTimestamp();
    }

    /**
     * Sets the timerange of the cells to yield.
     *
     * @see RowScanner#setMinTimestamp(long)
     * @param minTimestamp the minimum timestamp of the cells to yield
     * @param maxTimestamp the maximum timestamp of the cells to yield
     * @return this {@link RowScanner} to facilitate method chaining
     */
    public RowScanner setTimeRange(final long minTimestamp, final long maxTimestamp) {
        scanner.setTimeRange(minTimestamp, maxTimestamp);
        return this;
    }

    /**
     * Get the key of the current row being scanned.
     *
     * @see RowScanner#getCurrentKey()
     * @return the key of the current row
     */
    public byte[] getCurrentKey() {
        return scanner.getCurrentKey();
    }

    /**
     * Closes this Scanner
     *
     * @see RowScanner#close()
     * @return a Deferred indicating when the close operation has completed
     */
    public Deferred<Object> close() {
        final TimerContext ctx = metrics.getCloses().time();
        return scanner.close().addBoth(new TimerStoppingCallback<Object>(ctx));
    }

    /**
     * Scans the next batch of rows
     *
     * @see RowScanner#nextRows()
     * @return next batch of rows that were scanned
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
        final TimerContext ctx = metrics.getScans().time();
        return scanner.nextRows()
                .addBoth(new TimerStoppingCallback<ArrayList<ArrayList<KeyValue>>>(ctx));
    }

    /**
     * Scans the next batch of rows
     *
     * @see RowScanner#nextRows(int)
     * @param rows maximum number of rows to retrieve in the batch
     * @return next batch of rows that were scanned
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(final int rows) {
        final TimerContext ctx = metrics.getScans().time();
        return scanner.nextRows(rows)
                .addBoth(new TimerStoppingCallback<ArrayList<ArrayList<KeyValue>>>(ctx));
    }
}
