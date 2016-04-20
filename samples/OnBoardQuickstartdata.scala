%AddJar https://github.com/sathipal/spark-streaming-mqtt-with-security_2.10-1.3.0/releases/download/0.0.1/spark-streaming-mqtt-security_2.10-1.3.0-0.0.1.jar -f
%AddJar https://repo.eclipse.org/content/repositories/paho-snapshots/org/eclipse/paho/org.eclipse.paho.client.mqttv3/1.0.3-SNAPSHOT/org.eclipse.paho.client.mqttv3-1.0.3-20160319.041432-361.jar -f
%AddJar http://central.maven.org/maven2/com/google/code/gson/gson/2.2.4/gson-2.2.4.jar -f

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
val lines = MQTTUtils.createStream(ssc, 
            "tcp://quickstart.messaging.internetofthings.ibmcloud.com:1883",
            "iot-2/type/iotqs-sensor/id/c5278f58d377/evt/+/fmt/+", 
            "a:quickstart:hrcl78")
/*
 * The message topic and payload is split with space, so lets split the message with space
 * and keep the deviceId as key and payload as value.
 */
val deviceMappedLines = lines.map(x => ((x.split(" ", 2)(0)).split("/")(4), x.split(" ", 2)(1)))

// Map the Json payload into scala map
val jsonLines = deviceMappedLines.map(x => {
    var dataMap:Map[String, Any] = Map()
	val payload = new JsonParser().parse(x._2).getAsJsonObject()
    val deviceObject = payload.get("d").getAsJsonObject()
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

jsonLines.print()
ssc.start()
ssc.awaitTermination()
