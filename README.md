# Generating Hfile from TSV/CSV file

## build jar cp/sftp cloudera cdh service
```
mvn clean package
cp target/import-tsv-0.0.1.jar /opt/cloudera/parcels/CDH-5.13.1-1.cdh5.13.1.p0.2/lib/hbase/lib
```

## Uploaded test.csv To HDFS
```
cat << EOF >test.csv 
a1,b1,c1
a2,b2
a3,,c3
a4,b4,null
EOF
hdfs dfs -mkdir -p /tmp/src
hdfs dfs -put test.csv /tmp/src
```
## HBase table splits Region(5 node)
```
create 'test',{NAME => 'T', VERSIONS => 10},SPLITS => ['3|','6|','9|','c|']  
```

## Generating Hfile (rowkey=HmacMD5(privateKeyStr,column[T:b]))
```
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
-Dimporttsv.separator=, \
-Dimporttsv.skip.empty.columns=true \
-Dimporttsv.columns=HBASE_ROW_KEY,T:a,T:b,T:c \
-Dgenerate.rowkey=2 \
-Dhmacmd5.secretKey=privateKeyStr \
-Dimporttsv.bulk.output=/tmp/test \
'test' /tmp/src/test.csv
```

## Load Incremental HFiles
```
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
/tmp/test test
```