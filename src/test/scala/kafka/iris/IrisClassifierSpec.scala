import kafka.iris.{IrisStreamClassifier, IrisModel}
import org.scalatest._
import java.util.Properties
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


class IrisSpec extends FlatSpec with Matchers {
  "An Iris Classifier" should "return the class Iris-setosa" in {
    val prediction = IrisModel.score(5.1, 3.5, 1.4, 0.2)
    prediction should be ("Iris-setosa")
  }
  it should "return the class Iris-versicolor" in {
    val prediction = IrisModel.score(6.2, 2.2, 4.5, 1.5)
    prediction should be ("Iris-versicolor")
  }
  it should "return the class Iris-virginica" in {
    val prediction = IrisModel.score(6.1, 2.6, 5.6, 1.4)
    prediction should be ("Iris-virginica")
  }
}


class IrisClassifierSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    val config: Properties = {
        val p = new Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "integration-test")
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
        p
    }

    val driver = new TopologyTestDriver(
        IrisStreamClassifier.irisStreamClassifier("input-topic", "output-topic"), config)

    val recordFactory = new ConsumerRecordFactory("input-topic", new StringSerializer(), new StringSerializer())

    override def afterAll() {
        driver.close()
    }
    
    "Iris Stream Classifier" should "return the class Iris-setosa" in {
        driver.pipeInput(recordFactory.create("5.1,3.5,1.4,0.2"))
        val record: ProducerRecord[String, String] = driver.readOutput("output-topic", new StringDeserializer(), new StringDeserializer())
        record.value() should be("5.1,3.5,1.4,0.2,Iris-setosa")
    }
    it should "return the class Iris-versicolor" in {
        driver.pipeInput(recordFactory.create("6.2,2.2,4.5,1.5"))
        val record: ProducerRecord[String, String] = driver.readOutput("output-topic", new StringDeserializer(), new StringDeserializer())
        record.value() should be("6.2,2.2,4.5,1.5,Iris-versicolor")   
    }
    it should "return the class Iris-virginica" in {
        driver.pipeInput(recordFactory.create("6.1,2.6,5.6,1.4"))
        val record: ProducerRecord[String, String] = driver.readOutput("output-topic", new StringDeserializer(), new StringDeserializer())
        record.value() should be("6.1,2.6,5.6,1.4,Iris-virginica")   
    }
}