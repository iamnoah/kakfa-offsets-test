import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.io.Files;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingServer;
import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.common.TopicAndPartition;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaHealthcheck;
import kafka.server.KafkaServer;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by noah on 6/22/15.
 */
public class ManualOffsetCommitTest {

	private static final String TOPIC = "topic";
	public static final int PARTITIONS = 1;
	public static final String GROUP_ID = "groupId";

	private List<Integer> messages;

	private static KafkaServer kafkaServer;
	private static TestingServer zkServer;

	@BeforeClass
	public static void setupServers() throws Exception {
		zkServer = new TestingServer();

		final Properties props = new Properties();
		File tmpDir = Files.createTempDir();
		props.putAll(ImmutableMap.of(
				"port", "" + InstanceSpec.getRandomPort(),
				"broker.id", "1",
				"offsets.topic.replication.factor", "1",
				"log.dir", tmpDir.getCanonicalPath(),
				"zookeeper.connect", zkServer.getConnectString()
		));
		KafkaConfig kafkaConfig = new KafkaConfig(props);
		kafkaServer = new KafkaServer(kafkaConfig, new SysTime());

		kafkaServer.startup();
	}

	@AfterClass
	public static void destroyTestServers() throws IOException {
		if (kafkaServer != null) {
			kafkaServer.shutdown();
			kafkaServer.awaitShutdown();
		}
		if (zkServer != null) {
			zkServer.close();
		}
	}

	@Before
	public void startConsumer() throws InterruptedException {
		waitForTopic();

		messages = Lists.newArrayList();

		final ExecutorService threadPool = Executors.newFixedThreadPool(1);

		final ConsumerConnector connector = buildConnector();
		final List<KafkaStream<byte[], byte[]>> streams = connector.createMessageStreams(ImmutableMap.of(TOPIC, PARTITIONS)).get(TOPIC);
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			final ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
			final OffsetCommitChannel offsetCommitChannel = new OffsetCommitChannel();
			threadPool.submit(new Runnable() {
				@Override
				public void run() {
					consume(iterator, offsetCommitChannel);
				}
			});
		}
	}

	public void consume(ConsumerIterator<byte[], byte[]> iterator, OffsetCommitChannel offsetCommitChannel) {
		int i = 0;
		while(iterator.hasNext()) {
			final MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

			messages.add((int) messageAndMetadata.message()[0]);

			// commit on every 5th message
			if((++i % 5) == 0) {
				offsetCommitChannel.commitOffsets(
						GROUP_ID,
						ImmutableMap.of(
								new TopicAndPartition(new Tuple2<String, Object>(
										TOPIC,
										messageAndMetadata.partition()
								)),
								messageAndMetadata.offset()
						)
				);
			}
		}
	}

	public ConsumerConnector buildConnector() {
		Properties properties = new Properties();
		properties.putAll(ImmutableMap.<String, String>builder()
				.put("group.id", GROUP_ID)
				.put("offsets.storage", "kafka")
				.put("dual.commit.enabled", "false")
				.put("zookeeper.connect", zkServer.getConnectString())
				.put("zookeeper.session.timeout.ms", "400")
				.put("zookeeper.sync.time.ms", "200")
				.put("auto.commit.enable", "false")
				.put("auto.offset.reset", "largest")
				.build());
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}


	private void waitForTopic() throws InterruptedException {
		while (true) {
			KafkaHealthcheck kafkaHealthcheck = kafkaServer.kafkaHealthcheck();
			if (kafkaHealthcheck == null) {
				Thread.sleep(1000);
			} else {
				ZkClient zkClient = kafkaServer.apis().zkClient();
				AdminUtils.createTopic(zkClient, TOPIC, PARTITIONS, 1, new Properties());
				waitForTopicAvailablity(zkClient);
				break;
			}
		}
	}

	private void waitForTopicAvailablity(ZkClient zkClient) throws InterruptedException {
		boolean topicAvailable;
		int i = 0;
		do {
			topicAvailable = true;
			Thread.sleep(500);
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(TOPIC, zkClient);
			for(PartitionMetadata pm : JavaConversions.asJavaIterable(topicMetadata.partitionsMetadata().toIterable())) {
				topicAvailable &= pm.leader().isDefined();
			}
			if (i++ > 20) {
				fail("Topic not ready in time.");
			}
		} while (!topicAvailable);
	}

	@Test
	public void testOffsetsSurviveBrokerDowntime() throws InterruptedException {
		Properties props = new Properties();
		props.putAll(ImmutableMap.<String, String>builder()
				.put("metadata.broker.list", "localhost:" + kafkaServer.config().advertisedPort())
				.put("serializer.class", DefaultEncoder.class.getCanonicalName())
				.put("key.serializer.class", DefaultEncoder.class.getCanonicalName())
				.put("request.required.acks", "1")
				.build());
		final Producer<byte[], byte[]> producer = new Producer<>(new ProducerConfig(props));

		final int total = 24;
		// first publish 6 messages
		final int mid = 6;

		final List<Integer> all = FluentIterable.from(ContiguousSet.create(Range.closedOpen(0, total), DiscreteDomain.integers()))
				.toList();
		for (Integer i : all.subList(0, mid)) {
			producer.send(new KeyedMessage<>(
					TOPIC,
					new byte[]{i.byteValue()},
					new byte[]{i.byteValue()}
			));
		}

		// shutdown kafka
		kafkaServer.shutdown();
		kafkaServer.awaitShutdown();

		Thread.sleep(1000);

		// bring it back up
		kafkaServer.startup();
		waitForTopicAvailablity(kafkaServer.apis().zkClient());

		// publish the rest
		for (Integer i : all.subList(mid, all.size())) {
			producer.send(new KeyedMessage<>(
					TOPIC,
					new byte[]{i.byteValue()},
					new byte[]{i.byteValue()}
			));
		}

		// did we get all the messages?
		int tries = 0;
		while(messages.size() < total && tries++ < 20) {
			Thread.sleep(200);
		}
		assertEquals(ImmutableSet.copyOf(all), ImmutableSet.copyOf(messages));
	}


	private static class SysTime implements kafka.utils.Time {

		private SystemTime time = new SystemTime();

		@Override
		public long milliseconds() {
			return time.milliseconds();
		}

		@Override
		public long nanoseconds() {
			return time.nanoseconds();
		}

		@Override
		public void sleep(long ms) {
			time.sleep(ms);
		}
	}
}
