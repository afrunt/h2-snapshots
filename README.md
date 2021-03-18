[![Build Status](https://travis-ci.org/afrunt/h2-snapshots.svg?branch=main)](https://travis-ci.org/afrunt/h2-snapshots)
## Java library that helps you to create simple H2 Database snapshots
Add `H2-Snaphosts` to your project. For maven projects just add this dependency:
```xml
<dependency>
    <groupId>com.afrunt.h2s</groupId>
    <artifactId>h2-snapshots</artifactId>
    <version>0.4</version>
</dependency>
```
  
### Usage
Basically, you need to create the instance of the `H2Snapshot` class in order to create the snapshot of the current state. 
```java
import com.afrunt.h2s.H2Snapshot;
import com.sql.DataSource;

// ...
final H2Snapshot initialStateSnapshot = new H2Snapshot(dataSource);
// ...
```
To move database to the previous state, just invoke the `apply(...)` method like shown below
```java
import com.afrunt.h2s.H2Snapshot;
import com.sql.DataSource;

// Remember the previous state
final H2Snapshot initialStateSnapshot = new H2Snapshot(dataSource);

// ...
// Apply previously created snapshot to db
initialStateSnapshot.apply(dataSource);
// ...
```
