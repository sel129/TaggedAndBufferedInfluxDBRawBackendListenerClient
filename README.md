# TaggedAndBufferedInfluxDBRawBackendListenerClient
A JMeter backend listener that builds upon the built in InfluxDBRawBackendListenerClient.  This client does everything that the original InfluxDBRawBackendListenerClient does.  In addition, it allows you to tag samples and buffer samples before sending.

## Installation
Copy the .jar from the [latest release](https://github.com/sel129/TaggedAndBufferedInfluxDBRawBackendListenerClient/releases) to the lib/ext directory in your JMeter installation. 

## Configuration
Create a Backend Listener in JMeter and select the `org.apache.jmeter.visualizers.backend.influxdb.TaggedAndBufferedInfluxDBRawBackendListenerClient` client.  Configure the client parameters exactly as you would configure the InfluxDBRawBackendListenerClient.

### Buffered Samples
You will see an additional parameter named `batchSeconds`.  This listener doesn't immedietly send samples to influx, instead buffering them.  The value of this parameter defines the minimum length of time JMeter will wait before sending samples.  

<img width="1383" height="523" alt="image" src="https://github.com/user-attachments/assets/23932cdb-ed12-49a9-9489-b5083b4b8453" />
  
This listener will log the number of samples being sent with a message like:

```
INFO o.a.j.v.b.i.TaggedAndBufferedInfluxDBRawBackendListenerClient: Sending 31 samples to InfluxDB.
```

In the jmter.log file.  You can use these logs to tune the `batchSeconds` parameters to your needs.

### Adding Tags
Adding Tags to your samples can be done by creating additional parameters with the `TAG_` prefix.  For example, creating a parameter with the name `TAG_application` and a value of `myApplication` will tag all samples with an `application` tag and give them the value `myApplication`.
<img width="1343" height="613" alt="image" src="https://github.com/user-attachments/assets/323575be-92af-44d8-98a4-6405e58259cf" />

