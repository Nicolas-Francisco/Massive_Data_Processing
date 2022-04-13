package org.mdp.kafka.cli;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mdp.kafka.def.KafkaConstants;

public class KafkaExample {
	private final String topic;

	public KafkaExample(String topic) {
		this.topic = topic;
	}

	public void consume() {
		Properties props = KafkaConstants.PROPS;

		// randomise consumer ID so messages cannot be read by another consumer
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));
		try{
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n",
							record.topic(), record.partition(),
							record.offset(), record.key(), record.value());
				}
			}
		} finally{
			consumer.close();
		}
	}

	public void produce() {
		Thread one = new Thread() {
			public void run() {
				Producer<String, String> producer = new KafkaProducer<String, String>(KafkaConstants.PROPS);
				try {
					int i = 0;
					while(true) {
						Date d = new Date();
						producer.send(new ProducerRecord<String,String>(topic, 0, System.currentTimeMillis(), Integer.toString(i), d.toString()));
						Thread.sleep(1000);
						i++;
					}
				} catch (InterruptedException v) {
					System.err.println(v);
				} finally {
					producer.close();
				}
			}
		};
		one.start();
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: [topicName]");
			return;
		}
		KafkaExample c = new KafkaExample(args[0]);
		c.produce();
		c.consume();
	}
}
