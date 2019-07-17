---
page_type: sample
description: "A basic Java-based User Defined Function (UDF) for Hive."
languages:
- java
products:
- azure
- azure-hdinsight
---

# A UDF that converts various date/time formats to Hive timestamp format

A basic Java-based User Defined Function (UDF) for Hive.
This UDF converts various timestamps(SQL,ISO,.NET formats) into the [Hive Timestamp](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-Timestamps) format.

The various formats that it converts are:

* y/m/d
* y/d/m
* m/d/y
* m/y/d
* d/y/m
* d/m/y
* yyyy-mm-ddthh:mm:ss[.mmm]
* YYYYMMDD[ hh:mm:ss[.mmm]]

## About the code

The file __timestampconv.java__ uses regex to convert various different timestamp formats, and extracts the relavant information from it. This information is then parsed into the Hive timestamp format.

The file __timestampconvTest.java__ contains the JUNIT test cases which, validate the various formats that this example handles.

## Build the sample

NOTE: This sample is built using [Apache Maven](https://maven.apache.org), and also requires [Java](https://www.java.com) 1.7 or higher.

1. Clone or download this project.

2. From a command-line in the project directory, use the following to build, test, and package the project.

        mvn clean package

Once the process completes, a file named __hiveudf-1.0-SNAPSHOT.jar__ can be found in the __target__ directory.

## Deploy the sample to HDInsight

Copy the hiveudf-1.0-SNAPSHOT.jar file to your HDInsight cluster. There are a variety of methods that can be used to do this. See [Upload data for Hadoop jobs](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-upload-data/) for information on uploading the file directly to the storage account used by HDInsight.

You can also simply use __SCP__ to upload the data to the head node of a Linux-based cluster, then connect with SSH and use the `hdfs dfs -put` command to copy the data to storage for the cluster.

Ideally, you want the file in the default storage for the cluster; this way, it is accessible from all nodes in the cluster and is persisted when you delete the cluster.

## Use the sample

The following example assumes the following:

* The hiveudf-1.0-SNAPSHOT.jar has been stored into the /example/jars folder in the default storage account for the cluster.

* You are familiar with using Hive on HDInsight.

1. To create a source table where datetime information is stored as a string, use the following HiveQL statements:

    ```hiveql
    CREATE TABLE dateasstring (id int, datetime string);
    INSERT INTO dateasstring (id, datetime) values(1, "2017-11-07T01:35:00");
    ```

2. To create a new table where the `datetime` column is of type `timestamp`, use the following HiveQL:

    ```hiveql
    CREATE TABLE dateastimestamp (id int, datetime timestamp);
    ```

3. To register the `timestampconv` class as a function in Hive, use the following HiveQL statements:

    ```hiveql
    ADD JAR wasb:///example/jars/hiveudf-1.0-SNAPSHOT.jar';
    CREATE TEMPORARY FUNCTION timeconv AS 'com.microsoft.example.timestampconv';
    ```

4. To load data from the `dateasstring` table into the `dateastimestamp` table, while converting the date to a `timestamp`, use the following HiveQL:

    ```hiveql
    INSERT INTO TABLE dateastimestamp SELECT id, cast(timeconv(datetime, "yyyy-mm-ddthh:mm:ss[.mmm]") AS timestamp) FROM dateasstring;
    ```

    This statement inserts data into `dateastimestamp`, and casts the `datetime` column as type `timestamp`. The `timeconv` function is used to convert the date from string to `timestamp`.
