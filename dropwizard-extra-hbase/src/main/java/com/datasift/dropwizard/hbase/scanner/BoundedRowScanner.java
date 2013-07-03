package com.datasift.dropwizard.hbase.scanner;

import com.datasift.dropwizard.hbase.BoundedHBaseClient;
import com.datasift.dropwizard.hbase.util.PermitReleasingCallback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.ScanFilter;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * A Scanner that constraints concurrent requests with a {@link Semaphore}.
 * <p/>
 * To obtain an instance of a {@link RowScanner}, call {@link BoundedHBaseClient#scan(byte[])}.
 */
public class BoundedRowScanner implements RowScanner {

    private final RowScanner scanner;
    private final Semaphore semaphore;

    /**
     * Creates a new {@link BoundedRowScanner} for the given underlying {@link RowScanner},
     * constrained by the given {@link Semaphore}.
     *
     * @param scanner   the underlying {@link RowScanner} implementation
     * @param semaphore a {@link Semaphore} to contrains the maximum number of concurrent requests
     */
    public BoundedRowScanner(final RowScanner scanner, final Semaphore semaphore) {
        this.scanner = scanner;
        this.semaphore = semaphore;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @param key the first key to scan from (inclusive)
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setStartKey(byte[])
     */
    public RowScanner setStartKey(final byte[] key) {
        scanner.setStartKey(key);
        return this;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @param key the first key to scan from (inclusive)
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setStartKey(String)
     */
    public RowScanner setStartKey(final String key) {
        scanner.setStartKey(key);
        return this;
    }

    /**
     * Set the end key in the range to scan.
     *
     * @param key the end key to scan until (exclusive)
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setStopKey(byte[])
     */
    public RowScanner setStopKey(final byte[] key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the end key in the range to scan.
     *
     * @param key the end key to scan until (exclusive)
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setStopKey(byte[])
     */
    public RowScanner setStopKey(final String key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @param family the family to scan
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setFamily(byte[])
     */
    public RowScanner setFamily(final byte[] family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @param family the family to scan
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setFamily(String)
     */
    public RowScanner setFamily(final String family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @param qualifier the family to select from cells
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setQualifier(byte[])
     */
    public RowScanner setQualifier(final byte[] qualifier) {
        scanner.setQualifier(qualifier);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @param qualifier the family to select from cells
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setQualifier(String)
     */
    public RowScanner setQualifier(final String qualifier) {
        scanner.setQualifier(qualifier);
        return this;
    }

    public RowScanner setFilters(final ScanFilter... scanFilters) {
        scanner.setFilters(scanFilters);
        return this;
    }

    public RowScanner setFilters(final List<ScanFilter> scanFilters) {
        scanner.setFilters(scanFilters);
        return this;
    }

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @param regexp a regular expression to filter keys with
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setKeyRegexp(String)
     */
    public RowScanner setKeyRegexp(final String regexp) {
        scanner.setKeyRegexp(regexp);
        return this;
    }

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @param regexp a regular expression to filter keys with
     * @param charset the charset to decode the keys as
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setKeyRegexp(String)
     */
    public RowScanner setKeyRegexp(final String regexp, Charset charset) {
        scanner.setKeyRegexp(regexp, charset);
        return this;
    }

    /*public RowScanner setKeyRegexp(byte[] regexp, Charset charset) {
        scanner.setKeyRegexp(regexp, charset);
        return this;
    }

    public RowScanner setKeyRegexp(byte[] regexp) {
        return setKeyRegexp(regexp, Charsets.ISO_8859_1);
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

    public byte[] getPrefix(final byte[] prefix) {
        return scanner.getPrefix(prefix);
    }

    public byte[] getPrefix(final String prefix) {
        return getPrefix(prefix.getBytes());
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
    }*/

    /**
     * Set whether to use the server-side block cache during the scan.
     *
     * @param populateBlockcache whether to use the server-side block cache
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setServerBlockCache(boolean)
     */
    public RowScanner setServerBlockCache(final boolean populateBlockcache) {
        scanner.setServerBlockCache(populateBlockcache);
        return this;
    }

    /**
     * Set the maximum number of rows to fetch in each batch.
     *
     * @param maxRows the maximum number of rows to fetch in each batch
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setMaxNumRows(int)
     */
    public RowScanner setMaxNumRows(final int maxRows) {
        scanner.setMaxNumRows(maxRows);
        return this;
    }

    /**
     * Set the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @param maxKeyValues the maximum number of {@link KeyValue}s to fetch in each batch
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setMaxNumKeyValues(int)
     */
    public RowScanner setMaxNumKeyValues(final int maxKeyValues) {
        scanner.setMaxNumKeyValues(maxKeyValues);
        return this;
    }

    /**
     * Sets the minimum timestamp of the cells to yield.
     *
     * @param timestamp the minimum timestamp of the cells to yield
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setMinTimestamp(long)
     */
    public RowScanner setMinTimestamp(final long timestamp) {
        scanner.setMinTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the minimum timestamp of the cells to yield.
     *
     * @return the minimum timestamp of the cells to yield
     *
     * @see RowScanner#getMinTimestamp()
     */
    public long getMinTimestamp() {
        return scanner.getMinTimestamp();
    }

    /**
     * Sets the maximum timestamp of the cells to yield.
     *
     * @param timestamp the maximum timestamp of the cells to yield
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setMaxTimestamp(long)
     */
    public RowScanner setMaxTimestamp(final long timestamp) {
        scanner.setMaxTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the maximum timestamp of the cells to yield.
     *
     * @return the maximum timestamp of the cells to yield
     *
     * @see RowScanner#getMaxTimestamp()
     */
    public long getMaxTimestamp() {
        return scanner.getMaxTimestamp();
    }

    /**
     * Sets the timerange of the cells to yield.
     *
     * @param minTimestamp the minimum timestamp of the cells to yield
     * @param maxTimestamp the maximum timestamp of the cells to yield
     *
     * @return this {@link RowScanner} to facilitate method chaining
     *
     * @see RowScanner#setMinTimestamp(long)
     */
    public RowScanner setTimeRange(final long minTimestamp, final long maxTimestamp) {
        scanner.setTimeRange(minTimestamp, maxTimestamp);
        return this;
    }

    /**
     * Get the key of the current row being scanned.
     *
     * @return the key of the current row
     *
     * @see RowScanner#getCurrentKey()
     */
    public byte[] getCurrentKey() {
        return scanner.getCurrentKey();
    }

    /**
     *
     * @param attributes
     * @return
     */
    public RowScanner setAttributes(Map<String, String> attributes) {
        scanner.setAttributes(attributes);
        return this;
    }

    @Override
    public RowScanner addAttribute(String key, String value) {
        scanner.addAttribute(key, value);
        return this;
    }

    @Override
    public RowScanner addAttribute(String key, byte[] value) {
        scanner.addAttribute(key, value);
        return this;
    }

    /**
     * Closes this Scanner
     *
     * @return a Deferred indicating when the close operation has completed
     *
     * @see RowScanner#close()
     */
    public Deferred<Object> close() {
        semaphore.acquireUninterruptibly();
        return scanner.close()
                .addBoth(new PermitReleasingCallback<Object>(semaphore));
    }

    /**
     * Scans the next batch of rows
     *
     * @return next batch of rows that were scanned
     *
     * @see RowScanner#nextRows()
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
        semaphore.acquireUninterruptibly();
        return scanner.nextRows()
                .addBoth(new PermitReleasingCallback<ArrayList<ArrayList<KeyValue>>>(semaphore));
    }

    /**
     * Scans the next batch of rows
     *
     * @param rows maximum number of rows to retrieve in the batch
     *
     * @return next batch of rows that were scanned
     *
     * @see RowScanner#nextRows(int)
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(final int rows) {
        semaphore.acquireUninterruptibly();
        return scanner.nextRows(rows)
                .addBoth(new PermitReleasingCallback<ArrayList<ArrayList<KeyValue>>>(semaphore));
    }
}
