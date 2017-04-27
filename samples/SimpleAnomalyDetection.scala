%AddJar https://github.com/sathipal/spark-streaming-mqtt-with-security_2.10-1.3.0/releases/download/0.0.1/spark-streaming-mqtt-security_2.10-1.3.0-0.0.1.jar
%AddJar https://repo.eclipse.org/content/repositories/paho-snapshots/org/eclipse/paho/org.eclipse.paho.client.mqttv3/1.0.3-SNAPSHOT/org.eclipse.paho.client.mqttv3-1.0.3-20160514.041448-407.jar
%AddJar http://central.maven.org/maven2/com/google/code/gson/gson/2.2.4/gson-2.2.4.jar

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import com.google.gson.JsonObject
import com.google.gson.JsonParser

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.mqtt._
import org.apache.spark.SparkConf

import java.util.Map.Entry
import com.google.gson.JsonElement
import java.util.Set

import scala.collection.mutable.Map

val ssc = new StreamingContext(sc, Seconds(1))
ssc.checkpoint(".")

val lines = MQTTUtils.createStream(ssc,                                          // Spark Streaming Context
                "ssl://<your-org>.messaging.internetofthings.ibmcloud.com:8883", // Watson IoT Platform URL
                "iot-2/type/+/id/+/evt/+/fmt/+",                                // MQTT topic to receive the events
                "a:<your-org>:random",                                          // Unique ID of the application
                "<API-Key>",                                                    // API-Key
                "<Auth-Token>")                                                 // Auth-Token
                
/*
 * The message topic and payload is split with space, so lets split the message with space
 * and keep the deviceId as key and payload as value.
 */
val deviceMappedLines = lines.map(x => ((x.split(" ", 2)(0)).split("/")(4), x.split(" ", 2)(1)))

// Map the Json payload into scala map
val jsonLines = deviceMappedLines.map(x => {
    var dataMap:Map[String, Any] = Map()
    val payload = new JsonParser().parse(x._2).getAsJsonObject()
    var deviceObject = payload
    if(deviceObject.has("d")) {
        deviceObject = payload.get("d").getAsJsonObject()
    }
    val setObj = deviceObject.entrySet()
    val itr = setObj.iterator()
    while(itr.hasNext()) {
		val entry = itr.next();
        try {
            dataMap.put(entry.getKey(), entry.getValue().getAsDouble())
        } catch {
            case e: Exception => dataMap.put(entry.getKey(), entry.getValue().getAsString())
        }
    }
	(x._1, dataMap)
})

/**
  * Create a simple threshold rule. If cpu usage is greater than 10, alert
  */
   
 val  threasholdCrossedLines = jsonLines.filter(
       x => {
           var status = false;
           val value = x._2.get("cpu")
           if(value != null) {
               if(value.get.asInstanceOf[Double] > 0.1) {
                   status = true
               }
           }
           status
        })



threasholdCrossedLines.print()
ssc.start()
ssc.awaitTermination()
