# JH3lib: Java Interface for h3lib
This project uses JNA to provide native access to h3lib. [JNA Github](https://github.com/java-native-access/jna)

#### Compile JH3lib
- mvn compile

#### Create Jar 
- *(optional) use -DskipTests, to skip test phase*
- (Without dependencies) mvn package 
- (With dependencies) mvn package assembly:single

*jar is located in target directory*

#### Run Tests
- mvn test 

#### Install 
- mvn install
 
