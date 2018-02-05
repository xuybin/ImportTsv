/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package people.data;

import com.cloudera.io.netty.util.internal.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.CellCreator;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.InvalidLabelException;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import people.data.ImportTsv.TsvParser.BadTsvLineException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Write table content out to files in hdfs.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TsvImporterMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    /** Timestamp for all inserted rows */
    protected long ts;

    /** Column seperator */
    private String separator;

    /** Should skip bad lines */
    private boolean skipBadLines;
    /** Should skip empty columns*/
    private boolean skipEmptyColumns;
    private Integer generateRowkeyColumnIndex = -1;
    private String hMacMD5Secretkey="";
    private Counter badLineCount;
    private boolean logBadLines;

    protected ImportTsv.TsvParser parser;

    protected Configuration conf;

    protected String cellVisibilityExpr;

    protected long ttl;

    protected CellCreator kvCreator;

    private String hfileOutPath;

    public long getTs() {
        return ts;
    }

    public boolean getSkipBadLines() {
        return skipBadLines;
    }

    public Counter getBadLineCount() {
        return badLineCount;
    }

    public void incrementBadLineCount(int count) {
        this.badLineCount.increment(count);
    }

    /**
     * Handles initializing this class with objects specific to it (i.e., the parser).
     * Common initialization that might be leveraged by a subsclass is done in
     * <code>doSetup</code>. Hence a subclass may choose to override this method
     * and call <code>doSetup</code> as well before handling it's own custom params.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) {
        doSetup(context);

        conf = context.getConfiguration();
        parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY),
                separator);
        if (parser.getRowKeyColumnIndex() == -1) {
            throw new RuntimeException("No row key column specified");
        }
        this.kvCreator = new CellCreator(conf);
    }

    /**
     * Handles common parameter initialization that a subclass might want to leverage.
     * @param context
     */
    protected void doSetup(Context context) {

        Configuration conf = context.getConfiguration();

        generateRowkeyColumnIndex = conf.getInt(ImportTsv.GENERATE_ROWKEY, -1);
        hMacMD5Secretkey= conf.get(ImportTsv.HMACMD5_SECRETKEY, "");
        //System.err.println("generateRowkeyColumnIndex="+generateRowkeyColumnIndex);
        if (generateRowkeyColumnIndex >= 1) {
            generateRowkeyColumnIndex = generateRowkeyColumnIndex - 1;
        }
        // If a custom separator has been used,
        // decode it back from Base64 encoding.
        separator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
        if (separator == null) {
            separator = ImportTsv.DEFAULT_SEPARATOR;
        } else {
            separator = new String(Base64.decode(separator));
        }
        // Should never get 0 as we are setting this to a valid value in job
        // configuration.
        ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, 0);
        skipEmptyColumns = context.getConfiguration().getBoolean(
                ImportTsv.SKIP_EMPTY_COLUMNS, false);
        skipBadLines = context.getConfiguration().getBoolean(
                ImportTsv.SKIP_LINES_CONF_KEY, true);
        badLineCount = context.getCounter("ImportTsv", "Bad Lines");
        logBadLines = context.getConfiguration().getBoolean(ImportTsv.LOG_BAD_LINES_CONF_KEY, false);
        hfileOutPath = conf.get(ImportTsv.BULK_OUTPUT_CONF_KEY);
    }

    public static byte[] subBytes(byte[] src, int begin, int count) {
        byte[] bs = new byte[count];
        System.arraycopy(src, begin, bs, 0, count);
        return bs;
    }

    public static String encryptHmac(String key, String data) {
        try {
            SecretKey secretKey = new SecretKeySpec(Bytes.toBytes(key), "HmacMD5");
            Mac mac = Mac.getInstance("HmacMD5");
            mac.init(secretKey);
            byte[] resultBytes = mac.doFinal(Bytes.toBytes(data));
            String resultString = org.apache.hadoop.util.StringUtils.byteToHexString(resultBytes);
            return resultString;
        } catch (Exception ex) {
            return null;
        }
    }

    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2, int byte_2_length) {
        byte[] byte_3 = new byte[byte_1.length + byte_2_length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2_length);
        return byte_3;
    }

    /**
     * Convert a line of TSV text into an HBase table row.
     */
    @Override
    public void map(LongWritable offset, Text value,
                    Context context)
            throws IOException {
        byte[] lineBytes = value.getBytes();
        int rowLen = 0;
        if (generateRowkeyColumnIndex != -1) {
            //生成rowKey
            String line = Bytes.toString(lineBytes);
            String[] fields = StringUtils.split(line, separator);
            //System.err.println("line="+line);
            //System.err.println("fields.length="+fields.length);
            if (fields == null || generateRowkeyColumnIndex >= fields.length) {
                if (logBadLines) {
                    System.err.println(value);
                }
                if (skipBadLines) {
                    System.err.println(
                            "Bad line at offset: " + offset.get() + ":\n" + "Unable to generate key from (" + line + ")[" + generateRowkeyColumnIndex + "]");
                    incrementBadLineCount(1);
                    return;
                } else {
                    throw new IOException("Unable to generate key from (" + line + ")[" + generateRowkeyColumnIndex + "]");
                }
            } else if (StringUtils.isBlank(fields[generateRowkeyColumnIndex])) {
                if (logBadLines) {
                    System.err.println(value);
                }
                if (skipBadLines) {
                    System.err.println(
                            "Bad line at offset: " + offset.get() + ":\n" + "Can not use blank to generate key");
                    incrementBadLineCount(1);
                    return;
                } else {
                    throw new IOException("Can not use blank to generate key");
                }
            } else {
                //回填rowKey
                String rowKey = encryptHmac(hMacMD5Secretkey, fields[generateRowkeyColumnIndex]);
                lineBytes = byteMerger(Bytes.toBytes(rowKey + separator), lineBytes, value.getLength());
                rowLen = lineBytes.length - value.getLength();
                //System.err.println("line="+Bytes.toString(lineBytes));
            }
        }

        try {
            ImportTsv.TsvParser.ParsedLine parsed = parser.parse(
                    lineBytes, value.getLength() + rowLen);
            ImmutableBytesWritable rowKey =
                    new ImmutableBytesWritable(lineBytes,
                            parsed.getRowKeyOffset(),
                            parsed.getRowKeyLength());
            // Retrieve timestamp if exists
            ts = parsed.getTimestamp(ts);
            cellVisibilityExpr = parsed.getCellVisibility();
            ttl = parsed.getCellTTL();

            Put put = new Put(rowKey.copyBytes());
            for (int i = 0; i < parsed.getColumnCount(); i++) {
                if (i == parser.getRowKeyColumnIndex() || i == parser.getTimestampKeyColumnIndex()
                        || i == parser.getAttributesKeyColumnIndex() || i == parser.getCellVisibilityColumnIndex()
                        || i == parser.getCellTTLColumnIndex() || (skipEmptyColumns && (parsed.getColumnLength(i) == 0 || Bytes.toString(lineBytes, parsed.getColumnOffset(i), parsed.getColumnLength(i)).trim().equalsIgnoreCase("null")))) {
                    continue;
                }
                populatePut(lineBytes, parsed, put, i);
            }
            context.write(rowKey, put);
        } catch (InvalidLabelException badLine) {
            if (logBadLines) {
                System.err.println(value);
            }
            if (skipBadLines) {
                System.err.println(
                        "Bad line at offset: " + offset.get() + ":\n" +
                                badLine.getMessage());
                incrementBadLineCount(1);
                return;
            } else {
                throw new IOException(badLine);
            }
        } catch (BadTsvLineException badLine) {
            if (logBadLines) {
                System.err.println(value);
            }
            if (skipBadLines) {
                System.err.println(
                        "Bad line at offset: " + offset.get() + ":\n" +
                                badLine.getMessage());
                incrementBadLineCount(1);
                return;
            } else {
                throw new IOException(badLine);
            }
        } catch (IllegalArgumentException e) {
            if (logBadLines) {
                System.err.println(value);
            }
            if (skipBadLines) {
                System.err.println(
                        "Bad line at offset: " + offset.get() + ":\n" +
                                e.getMessage());
                incrementBadLineCount(1);
                return;
            } else {
                throw new IOException(e);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void populatePut(byte[] lineBytes, ImportTsv.TsvParser.ParsedLine parsed, Put put,
                               int i) throws BadTsvLineException, IOException {
        Cell cell = null;
        if (hfileOutPath == null) {
            cell = new KeyValue(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
                    parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
                    parser.getQualifier(i).length, ts, KeyValue.Type.Put, lineBytes,
                    parsed.getColumnOffset(i), parsed.getColumnLength(i));
            if (cellVisibilityExpr != null) {
                // We won't be validating the expression here. The Visibility CP will do
                // the validation
                put.setCellVisibility(new CellVisibility(cellVisibilityExpr));
            }
            if (ttl > 0) {
                put.setTTL(ttl);
            }
        } else {
            // Creating the KV which needs to be directly written to HFiles. Using the Facade
            // KVCreator for creation of kvs.
            List<Tag> tags = new ArrayList<Tag>();
            if (cellVisibilityExpr != null) {
                tags.addAll(kvCreator.getVisibilityExpressionResolver()
                        .createVisibilityExpTags(cellVisibilityExpr));
            }
            // Add TTL directly to the KV so we can vary them when packing more than one KV
            // into puts
            if (ttl > 0) {
                tags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(ttl)));
            }
            cell = this.kvCreator.create(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
                    parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
                    parser.getQualifier(i).length, ts, lineBytes, parsed.getColumnOffset(i),
                    parsed.getColumnLength(i), tags);
        }
        put.add(cell);
    }
}
