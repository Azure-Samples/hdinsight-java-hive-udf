# hdinsight-java-hive-udf
A basic Java-based User Defined Function (UDF) for Hive.
This UDF converts various timestamps(SQL,ISO,.NET formats) into HIVE TIMESTAMP format
The various formats that it convertrs are:
1)y/m/d
2)y/d/m
3)m/d/y
4)m/y/d
5)d/y/m
6)d/m/y
7)yyyy-mm-ddthh:mm:ss[.mmm]
8)YYYYMMDD[ hh:mm:ss[.mmm]]
## Running this sample
This is a mvn sample
mvn clean install should run the JUnit tests and a BUILD SUCCESS should be seen.
This would also create a target directory with a hiveudf-1.0-SNAPSHOT.jar
## Deploy this sample to Azure
Copy the hiveudf-1.0-SNAPSHOT.jar to the headnode of your cluster.
In a hive script do the following:
Here hiveudf1 is the name of the table and a1 is the column name where the udf is being applied to.

add jar hiveudf-1.0-SNAPSHOT.jar;
CREATE TEMPORARY  FUNCTION timeconv AS 'com.microsoft.example.timestampconv';
select cast (timeconv(a1,"yyyy-mm-ddthh:mm:ss[.mmm]") as timestamp) from hiveudf1;

## About the code
The file timestampconv.java employs regexes to convert various different timestamps 
and extracts the relavant information from it and parses it in the HIVE TIMESTAMP format.
The file timestampconvTest.java has various JUNIT test cases which validate the various formats
## More information
Coming soon...
