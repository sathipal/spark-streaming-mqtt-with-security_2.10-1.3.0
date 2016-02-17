======================================
Spark Streaming MQTT - Introduction
======================================

Introduction
-------------

This library helps in integrating any MQTT sources with Spark streaming securely in simple steps to process the events in real-time.

Apache Spark Streaming is an extension of the core Apache Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches.

With IoT, Big Data is moving towards more stream oriented as these devices send events at regular intervals (say every second) and they need to be processed in real-time, to detect anomalies, filter events, enrich the data for machine learning, prediction and etc.

We customized the spark-streaming-mqtt-connector library from Apache Spark and added the following,
* Added SSL security such that the connection between the Spark and IBM Watson IoT Platform is always secured.
* Added the topic in the RDD, such that one can parse and associate the messages with the appropriate device or device type. 

----

How to use in IBM Bluemix?
--------------------------

Follow the steps present in [this recipe](http://www.ibm.com/internet-of-things/) to onboard IoT device events to Apache Spark.

Create Streams
--------------

In this section we detail how to use this spark-streaming-mqtt-connector library to connect to [IBM Watson IoT Platform](http://www.ibm.com/internet-of-things/) and consume the device events.

As a first step, create a Streaming context by specifying the Spark configuration and batch interval,

    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
    
Then create an input DStream using MQTTUtils as follows,

    val lines = MQTTUtils.createStream(ssc, // Spark Streaming Context
                    "ssl://<your-org>.messaging.internetofthings.ibmcloud.com:8883", // Watson IoT Platform URL
                    "iot-2/type/+/id/+/evt/+/fmt/+", // MQTT topic to receive the events
                    "a:<your-org>:random", // Unique ID of the application
                    "<API-Key>", // API-Key
                    "<Auth-Token>") // Auth-Token

where the different parameters are formed as follows,

##### 1. IBM Watson IoT Platform URL #####

orgid.messaging.internetofthings.ibmcloud.com using TLS at port 8883, where org-id is the organization identifier, which is displayed in the portal when you signed in.

##### 2. MQTT Topic #####

The topic name used to receive events from one or more devices as follows,

    iot-2/type/device_type/id/device_id/evt/event_id/fmt/format_string

**Note**: The MQTT “any” wildcard character (+) may be used for any of the following components if you want to subscribe to more than one type of event, or events from more than a single device.

* device_type
* device_id
* event_id
* format_string

##### 3. MQTT Client Identifier #####

Supply a client ID of the form,

    a:<org-id>:<app-id>

* a indicates the client is an application.
* org_id is your unique organization ID, assigned when you sign up with the service. It will be a 6 character alphanumeric string.
* app_id is a user-defined unique string identifier for this client.

##### Multiple receivers #####

If you want to create multiple receivers (DStreams) to load balance device events across multiple executors, you can use the client ID in the following form,

    A:<org-id>:<app-id>

Note the difference in the first character, the block letter "A" means the application is a scalable application and load balances the events across multiple receivers.

Look at this recipe if you want to know more about the Scalable application development in IBM Watson IoT Platform.

#### 4 & 5. Auth key and token ####

* MQTT username with the API-Key's "Key" property
* MQTT password containing the API-Key's "Auth-Token" property 

Then, in the next step, add the code to parse the topic and associate the messages with deviceId as shown below, Also start the streaming context such that it runs for every batch interval that we specified earlier.

    /*
     * The message topic and payload is split with space, so lets split the message with space
     * and keep the deviceId as key and payload as value.
     */
    val deviceMappedLines = lines.map(x => ((x.split(” “, 2)(0)).split(“/”)(4), x.split(” “, 2)(1)))
    deviceMappedLines.print()
    ssc.start()
    ssc.awaitTermination()

##### 6. RDD storage level (Optional) #####

There are different storage levels offered by Spark, choose one appropriate for you. For more information about storage level, [read the Spark guide](http://spark.apache.org/docs/latest/programming-guide.html).  

If you don't specify a RDD storage level, then by default StorageLevel.MEMORY_AND_DISK_SER_2 is used.

----

Deploying Outside Bluemix
-------------------------

As with any Spark applications, spark-submit can be used to launch your application. 

For Scala and Java applications, if you are using SBT or Maven for project management, then package spark-streaming-mqtt-iotf_2.10 and its dependencies into the application JAR. Make sure spark-core_2.10 and spark-streaming_2.10 are marked as provided dependencies as those are already present in a Spark installation. Then use spark-submit to launch your application (see [Deploying section](http://spark.apache.org/docs/latest/streaming-programming-guide.html#deploying-applications) in the Apache Spark programming guide).

Alternatively, you can also download the JAR from [this link](https://github.com/sathipal/spark-streaming-mqtt-with-security_2.10-1.3.0/releases/download/0.0.1/spark-streaming-mqtt-security_2.10-1.3.0-0.0.1.jar) and add it to spark-submit with --jars as follows,


    ./bin/spark-submit –jar spark-streaming-mqtt-iotf.jar

----
