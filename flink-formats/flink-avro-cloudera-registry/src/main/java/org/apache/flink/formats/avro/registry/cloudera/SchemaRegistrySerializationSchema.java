/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.cloudera;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Preconditions;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * {@link KafkaSerializationSchema} for writing Avro records and registering the schema in the Schema Registry.
 */
public class SchemaRegistrySerializationSchema<K, V, I> implements KafkaSerializationSchema<I>, KafkaContextAware<I> {

	private final Map<String, ?> config;
	private final TopicSelector<K, V> topicSelector;
	private final KeySelector<I, K> keySelector;
	private final ValueSelector<I, V> valueSelector;

	private transient KafkaAvroSerializer valueSerializer;
	private transient KafkaAvroSerializer keySerializer;
	private SchemaRegistryClient registry;

	private SchemaRegistrySerializationSchema(Map<String, ?> config, TopicSelector<K, V> topicSelector, KeySelector<I, K> keySelector, ValueSelector<I, V> valueSelector) {
		this.config = config;
		this.topicSelector = Preconditions.checkNotNull(topicSelector);
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.valueSelector = Preconditions.checkNotNull(valueSelector);
	}

	/**
	 * Creates a builder for the {@link SchemaRegistrySerializationSchema} that writes incoming records to fixed target topic.
	 * Record schemas are registered and validated against the Schema Registry.
	 *
	 * @param topic Target topic for the messages
	 * @return Instance of the {@link Builder}
	 */
	public static <V> Builder<Void, V, V> builder(String topic) {
		return builder(new FixedTopicSelector<>(topic));
	}

	/**
	 * Creates a builder for the {@link SchemaRegistrySerializationSchema} that writes incoming records to a target topic that is extracted
	 * from the elements. Record schemas are registered and validated against the Schema Registry.
	 *
	 * @param topicSelector Extractor to be called for each (key,value) pair to get the target topic
	 * @return Instance of the {@link Builder}
	 */
	public static <K, V> Builder<K, V, V> builder(TopicSelector<K, V> topicSelector) {
		return new Builder<>(topicSelector, v -> v);
	}

	private void checkInitialized() {
		if (valueSerializer == null) {
			if (registry == null) {
				registry = new SchemaRegistryClient(config);
			}

			keySerializer = new KafkaAvroSerializer(registry);
			keySerializer.configure(config, true);

			valueSerializer = new KafkaAvroSerializer(registry);
			valueSerializer.configure(config, false);
		}
	}

	@VisibleForTesting
	protected void setRegistryClient(SchemaRegistryClient registry) {
		this.registry = registry;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(I record, @Nullable Long ts) {
		checkInitialized();
		Headers headers = new RecordHeaders();

		K key = getKey(record);
		V value = getValue(record);

		String targetTopic = getTargetTopic(record);

		byte[] serializedKey = key == null ? null : keySerializer.serialize(targetTopic, headers, key);
		byte[] serializedValue = valueSerializer.serialize(targetTopic, headers, value);

		return new ProducerRecord<byte[], byte[]>(targetTopic, null, serializedKey, serializedValue, headers);
	}

	private K getKey(I record) {
		try {
			return keySelector.getKey(record);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private V getValue(I record) {
		return valueSelector.apply(record);
	}

	@Override
	public String getTargetTopic(I record) {
		return topicSelector.apply(getKey(record), getValue(record));
	}

	private interface ValueSelector<I, V> extends Function<I, V>, Serializable {}

	/**
	 * Builder class for {@link SchemaRegistrySerializationSchema}.
	 */
	public static final class Builder<K, V, I> {

		private final Map<String, Object> config = new HashMap<>();
		private final TopicSelector<K, V> topicSelector;
		private KeySelector<I, K> keySelector = k -> null;
		private ValueSelector<I, V> valueSelector;

		private Builder(TopicSelector<K, V> topicSelector, ValueSelector<I, V> valueSelector) {
			this.topicSelector = topicSelector;
			this.valueSelector = valueSelector;
		}

		public Builder<K, V, I> setRegistryAddress(String schemaRegistryAddress) {
			config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryAddress);
			return this;
		}

		public Builder<K, V, I> setConfig(Map<String, ?> conf) {
			config.putAll(conf);
			return this;
		}

		public <X> Builder<X, V, I> setKey(KeySelector<I, X> keySelector) {
			Builder<X, V, I> b = (Builder<X, V, I>) this;
			b.keySelector = Preconditions.checkNotNull(keySelector);
			return b;
		}

		public <X> Builder<X, V, Tuple2<X, V>> setKey() {
			Builder<?, V, Tuple2<X, V>> b = ((Builder<?, V, Tuple2<X, V>>) this);
			b.valueSelector = t -> t.f1;
			return b.setKey(t -> t.f0);
		}

		private void validate() {
			if (!config.containsKey(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name())) {

				throw new IllegalArgumentException("Schema Registry address must be set either using .setRegistryAddress(..) or through the config.");
			}
		}

		public SchemaRegistrySerializationSchema<K, V, I> build() {
			validate();
			return new SchemaRegistrySerializationSchema<>(config, topicSelector, keySelector, valueSelector);
		}
	}

	private static class FixedTopicSelector<K, V> implements TopicSelector<K, V> {

		private final String topic;

		public FixedTopicSelector(String topic) {
			this.topic = topic;
		}

		@Override
		public String apply(K k, V v) {
			return topic;
		}
	}
}
