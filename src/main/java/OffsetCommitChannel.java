import java.io.IOException;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;
import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.network.BlockingChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for manging connections and communication with the Kafka offsets coordinator.
 */
class OffsetCommitChannel {

	private static final Logger LOG = LoggerFactory.getLogger(OffsetCommitChannel.class);

	public static final String CLIENT_ID = "OffsetCommitChannel";
	private static final short OFFSET_MANGER_VERSION = 1; // commit to kafka
	// id sent with requests
	private int correlationId;

	private final LoadingCache<String, BlockingChannel> channelCache = CacheBuilder.newBuilder()
			.removalListener(new RemovalListener<String, BlockingChannel>() {
				@Override
				public void onRemoval(RemovalNotification<String, BlockingChannel> notification) {
					final BlockingChannel channel = notification.getValue();
					if (channel != null) {
						channel.disconnect();
					}
				}
			}).build(new CacheLoader<String, BlockingChannel>() {
				@Override
				public BlockingChannel load(String key) throws IOException {
					return connectToOffsetManager(key);
				}
			});

	private BlockingChannel connectToOffsetManager(String consumerGroupId) throws IOException {
		BlockingChannel channel = new BlockingChannel(
				"localhost",
				2128,
				BlockingChannel.UseDefaultBufferSize(),
				BlockingChannel.UseDefaultBufferSize(),
				5000
		);
		channel.connect();

		channel.send(new ConsumerMetadataRequest(
				consumerGroupId,
				ConsumerMetadataRequest.CurrentVersion(),
				correlationId++,
				CLIENT_ID
		));
		ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

		if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
			final Broker offsetManager = metadataResponse.coordinator();
			if (!offsetManager.host().equals("localhost") || offsetManager.port() != 2128) {
				channel.disconnect();
				channel = new BlockingChannel(
						offsetManager.host(),
						offsetManager.port(),
						BlockingChannel.UseDefaultBufferSize(),
						BlockingChannel.UseDefaultBufferSize(),
						5000
				);
				channel.connect();
			}
			return channel;
		} else {
			LOG.error("Error {} connecting to offsets manager. Retrying...", metadataResponse.errorCode());
			backoff();
		}
		throw new IOException("Could not connect to offset manager");
	}

	/**
	 * Attempts to commit the given offsets for the given consumer group.
	 *
	 * @param consumerGroup the id of the consumer group.
	 * @param offsets the offsets to commit.
	 */
	public void commitOffsets(String consumerGroup, Map<TopicAndPartition, Long> offsets) {
		final long now = System.currentTimeMillis();
		final OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(
				consumerGroup,
				Maps.transformValues(offsets, new Function<Long, OffsetAndMetadata>() {
					@Override
					public OffsetAndMetadata apply(Long offset) {
						return new OffsetAndMetadata(offset, OffsetAndMetadata.NoMetadata(), now);
					}
				}),
				correlationId++,
				CLIENT_ID,
				OFFSET_MANGER_VERSION
		);
		int tries = 0;
		while(tries++ < 5) {
			try {
				final BlockingChannel channel = channelCache.getUnchecked(consumerGroup);
				final OffsetCommitResponse offsetCommitResponse;

				// we can only make 1 request+response at a time over the channel
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (channel) {
					channel.send(offsetCommitRequest.underlying());
					offsetCommitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
				}
				// SUCCESS! we're done
				if (!offsetCommitResponse.hasError()) {
					return;
				}

				for (Object error : offsetCommitResponse.errors().values()) {
					if (error == ErrorMapping.ConsumerCoordinatorNotAvailableCode() ||
							error == ErrorMapping.NotCoordinatorForConsumerCode()) {
						// disconnect and find the new coordinator
						throw new IOException("Offset coordinator changed for: " + consumerGroup);
					} else {
						LOG.error("Error commiting offsets: {}", error);
					}
				}
				// else just retry
			} catch (IOException e) {
				LOG.error("Error committing offsets. Reconnecting.", e);
				// if there is a problem with the channel, reconnect
				channelCache.invalidate(consumerGroup);
			}

			// wait before the next try
			backoff();
		}
	}

	private void backoff() {
		try {
			Thread.sleep(500);
		} catch (InterruptedException ignored) {
		}
	}

	public void close() {
		for (BlockingChannel channel : channelCache.asMap().values()) {
			channel.disconnect();
		}
	}
}
