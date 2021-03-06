package com.datasift.dropwizard.hbase.scanner;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client for scanning over a selection of rows.
 * <p/>
 * To obtain an instance of a {@link RowScanner}, call {@link
 * com.datasift.dropwizard.hbase.HBaseClient#scan(byte[])}.
 * <p/>
 * This implementation is a proxy for a {@link org.hbase.async.Scanner}.
 */
public class RowScannerProxy implements RowScanner {

    private final Scanner scanner;

    /**
     * Creates a new {@link RowScannerProxy} for the given {@link Scanner}.
     *
     * @param scanner the underlying {@link Scanner} to wrap
     */
    public RowScannerProxy(final Scanner scanner) {
        this.scanner = scanner;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @param key the first key to scan from (inclusive).
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStartKey(byte[])
     */
    public RowScanner setStartKey(final byte[] key) {
        scanner.setStartKey(key);
        return this;
    }

    /**
     * Set the first key in the range to scan.
     *
     * @param key the first key to scan from (inclusive).
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStartKey(String)
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
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     */
    public RowScanner setStopKey(final byte[] key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the end key in the range to scan.
     *
     * @param key the end key to scan until (exclusive).
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setStopKey(byte[])
     */
    public RowScanner setStopKey(final String key) {
        scanner.setStopKey(key);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @param family the family to scan.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setFamily(byte[])
     */
    public RowScanner setFamily(final byte[] family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the family to scan.
     *
     * @param family the family to scan.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setFamily(String)
     */
    public RowScanner setFamily(final String family) {
        scanner.setFamily(family);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @param qualifier the family to select from cells.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setQualifier(byte[])
     */
    public RowScanner setQualifier(final byte[] qualifier) {
        scanner.setQualifier(qualifier);
        return this;
    }

    /**
     * Set the qualifier to select from cells
     *
     * @param qualifier the family to select from cells.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setQualifier(String)
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
     * @param regexp a regular expression to filter keys with.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
     */
    public RowScanner setKeyRegexp(final String regexp) {
        scanner.setKeyRegexp(regexp);
        return this;
    }

    /**
     * Set a regular expression to filter keys being scanned.
     *
     * @param regexp a regular expression to filter keys with.
     * @param charset the charset to decode the keys as.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setKeyRegexp(String)
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

    /*public byte[] getKeyRegexp(byte[] regexp, Charset charset) {
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

    public RowScanner setFilterList(final byte[]... filters) {
        scanner.setFilterList(filters);
        return this;
    }

    public byte[] getPrefix(final byte[] prefix) {
        return scanner.getPrefix(prefix);
    }

    public byte[] getPrefix(final String prefix) {
        return getPrefix(prefix.getBytes());
    }

    @Override
    public byte[] getColumnPrefix(String prefix) {
        return getColumnPrefix(prefix.getBytes());
    }

    @Override
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
     * @param populateBlockcache whether to use the server-side block cache.
     *
     * @see org.hbase.async.Scanner#setServerBlockCache(boolean)
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     */
    public RowScanner setServerBlockCache(final boolean populateBlockcache) {
        scanner.setServerBlockCache(populateBlockcache);
        return this;
    }

    /**
     * Set the maximum number of rows to fetch in each batch.
     *
     * @param maxRows the maximum number of rows to fetch in each batch.
     *
     * @see org.hbase.async.Scanner#setMaxNumRows(int)
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     */
    public RowScanner setMaxNumRows(final int maxRows) {
        scanner.setMaxNumRows(maxRows);
        return this;
    }

    /**
     * Set the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @param maxKeyValues the maximum number of {@link KeyValue}s to fetch in each batch.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMaxNumKeyValues(int)
     */
    public RowScanner setMaxNumKeyValues(final int maxKeyValues) {
        scanner.setMaxNumKeyValues(maxKeyValues);
        return this;
    }

    /**
     * Sets the minimum timestamp of the cells to yield.
     *
     * @param timestamp the minimum timestamp of the cells to yield.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     */
    public RowScanner setMinTimestamp(final long timestamp) {
        scanner.setMinTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the minimum timestamp of the cells to yield.
     *
     * @return the minimum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMinTimestamp()
     */
    public long getMinTimestamp() {
        return scanner.getMinTimestamp();
    }

    /**
     * Sets the maximum timestamp of the cells to yield.
     *
     * @param timestamp the maximum timestamp of the cells to yield.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMaxTimestamp(long)
     */
    public RowScanner setMaxTimestamp(final long timestamp) {
        scanner.setMaxTimestamp(timestamp);
        return this;
    }

    /**
     * Gets the maximum timestamp of the cells to yield.
     *
     * @return the maximum timestamp of the cells to yield.
     *
     * @see org.hbase.async.Scanner#getMaxTimestamp()
     */
    public long getMaxTimestamp() {
        return scanner.getMaxTimestamp();
    }

    /**
     * Sets the timerange of the cells to yield.
     *
     * @param minTimestamp the minimum timestamp of the cells to yield.
     * @param maxTimestamp the maximum timestamp of the cells to yield.
     *
     * @return this {@link RowScanner} to facilitate method chaining.
     *
     * @see org.hbase.async.Scanner#setMinTimestamp(long)
     */
    public RowScanner setTimeRange(final long minTimestamp, final long maxTimestamp) {
        scanner.setTimeRange(minTimestamp, maxTimestamp);
        return this;
    }

    /**
     * Get the key of the current row being scanned.
     *
     * @return the key of the current row.
     *
     * @see org.hbase.async.Scanner#getCurrentKey()
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
        Map<String, byte[]> attrs = Maps.newHashMap();
        for (Map.Entry<String, String> entry: attributes.entrySet()) {
            attrs.put(entry.getKey(), entry.getValue().getBytes());
        }
        scanner.setAttributes(attrs);
        return this;
    }

    public RowScanner addAttribute(String key, String value) {
        addAttributeInternal(key, value.getBytes());
        return this;
    }

    public RowScanner addAttribute(String key, byte[] value) {
        addAttributeInternal(key, value);
        return this;
    }

    private void addAttributeInternal(String key, byte[] value) {
        if (scanner.getAttributes() == null) {
            scanner.setAttributes(new HashMap<String, byte[]>());
        }
        scanner.getAttributes().put(key, value);
    }

    /**
     * Closes this Scanner
     *
     * @return a Deferred indicating when the close operation has completed.
     *
     * @see org.hbase.async.Scanner#close()
     */
    public Deferred<Object> close() {
        return scanner.close();
    }

    /**
     * Scans the next batch of rows
     *
     * @return next batch of rows that were scanned.
     *
     * @see org.hbase.async.Scanner#nextRows()
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
        return scanner.nextRows();
    }

    /**
     * Scans the next batch of rows
     *
     * @param rows maximum number of rows to retrieve in the batch.
     *
     * @return next batch of rows that were scanned.
     *
     * @see org.hbase.async.Scanner#nextRows(int)
     */
    public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(final int rows) {
        return scanner.nextRows(rows);
    }
}
