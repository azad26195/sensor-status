# Sensor Status
> This is a mini project created as the part of my college assignment.

 > ##### Input
 We get the following tuple from Monitoring web:<br/>
If the sensor is **UP**:<br/>
`Version, SensorID, ServerTS`<br/>
If the sensor is **DOWN**:<br/>
*We do not get any tuple.* <br/>
##### Logic
Our Job is to process the sensor data.
For every sensor we need tuples sorted by Ts.
We have two variables: cronFrequency and noDataThreshold.
As long as we have consecutive tuples with approximately cronFrequency the sensor is ON.
If we miss tuples, and if we do not get tuples from a sensor for a time period as large as
noDataThreshold then the sensor status needs to be flipped to OFF.
As soon as we start getting data after that, sensor status should again be ON.
Final output should be timeRanges with sensorStatus ON & OFF
For ex.

| From | TO | STATUS |
|-------|-------|----|
| 12:01 |­ 12:12 | ON |
| 12:13 ­| 12:19 | OFF|
| 12:19 ­| 12:29 | ON |
We can round it off to minute level because our lowest interval of getting monitoring data from
sensors will be 3 minute.

### Pre - Requriements

- Hadoop
- Cascading Map Reduce Framework
- Gradle
- JAVA
- > #### External Library used
  -  org.joda.time

### Run Commands

```
gradle clean jar

hadoop jar ./build/libs/sensor-status.jar input.txt /output
```
