# JH3lib: 
A Java suite containing modules related to h3lib. Project was created using Maven 3.6.0

#### Compile all modules:
- mvn compile

#### Test all modules:
- mvn test

#### Package all modules(create  .jar):
- mvn package
- packages are created in each module's target folder

#### Install all modules:
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
