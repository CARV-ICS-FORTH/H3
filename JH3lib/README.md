# H3 Java wrapper and tests

A Java suite containing modules related to h3lib. Project was created using Maven 3.6.0. Java version 9 or higher is required.

#### Compile all modules
- mvn compile

#### Test all modules
- mvn test

#### Package all modules (create .jar)
- mvn package
- packages are created in each module's `target` folder

#### Install all modules
- mvn install

*To compile/package/install specific modules use -pl flag and the name of the module(s).
e.g mvn -pl JH3,JH3Interface compile*

*Testing phase can be skipped by using -DskipTests flag*

### JH3Interface:
A Java Interface for h3lib. This project uses [JNA](https://github.com/java-native-access/jna) to provide native access
 to h3lib. Navigate into the module's folder to compile/package/install.

#### Compile module
- mvn compile

#### Test module
- mvn test

#### Package module
- mvn package

#### Build only package with dependencies
- mvn compile assembly:single

#### Install module
- mvn install

### JH3:
Uses JH3Interface to provide a class to make h3lib access more Java-like.
Instructions are same as JH3Interface module.

### JH3HadoopFS:
A H3lib plugin for Hadoop FileSystem API.
To specify the back-end storage, set the environment variable H3LIB\_STORAGE\_URI.

#### How to add JH3HadoopFS to Hadoop:
```$xslt
cp path/to/JH3lib/JH3HadoopFS/target/<JH3HadoopFS-jar-with-dependencies.jar> path/to/hadoop/share/common/lib
cd path/to/built-hadoop/etc/hadoop
open core-site.xml and add the following configuration elements:
<property>
    <name>fs.h3.impl</name>
    <value>gr.forth.ics.JH3lib.JH3HadoopFS</value>
    <description>The filesystem for H3lib URIs </descrition>
</property>

<property>
    <name>fs.AbstractFileSystem.h3.impl</name>
    <value>gr.forth.ics.JH3lib.JH3HadoopAbstractFS</value>
    <description>The AbstractFileSystem for H3lib</description>
</property>
```

#### How to use JH3HadoopFS in Spark:
```$xslt
cd path/to/spark/conf
cp spark-env.sh.template spart-env.sh
add to spark-env:
 export SPARK_DIST_CLASSPATH=$(/path/to/built-hadoop/bin/hadoop classpath)
```
When ``h3://`` is used as the schema part of the URI in Spark,
Hadoop will use the plugin instead of hdfs, local etc.
See spark-examples folder under JH3HadoopFS module.
