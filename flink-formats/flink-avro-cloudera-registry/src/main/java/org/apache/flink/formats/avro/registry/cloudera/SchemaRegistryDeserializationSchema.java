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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Preconditions;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link KafkaDeserializationSchema} for reading Avro records with their schemas registered in the Schema Registry.
 */
public abstract class SchemaRegistryDeserializationSchema<K, V, O> implements KafkaDeserializationSchema<O> {

	private final Map<String, Object> baseConfig;
	private transient SchemaRegistryClient registry;
	private final String keyReaderSchema;
	private final String valueReaderSchema;
	private transient Map<String, Tuple2<KafkaAvroDeserializer, KafkaAvroDeserializer>> deserializers;

	protected SchemaRegistryDeserializationSchema(Schema keyReaderSchema, Schema valueReaderSchema, Map<String, Object> baseConfig) {
		this.baseConfig = baseConfig;
		this.keyReaderSchema = keyReaderSchema != null ? keyReaderSchema.toString() : null;
		this.valueReaderSchema = valueReaderSchema.toString();
	}

	/**
	 * Creates a builder for {@link SchemaRegistryDeserializationSchema} that produces classes that were generated from avro
	 * schema and looks up writer schema in Schema Registry.
	 *
	 * @param recordClazz class of record to be produced
	 * @return A new schema builder
	 */
	public static <T> Builder<Void, T, T> builder(Class<T> recordClazz) {
		return new Builder<>(recordClazz, getSchema(recordClazz));
	}

	/**
	 * Creates a builder for {@link SchemaRegistryDeserializationSchema} that produces {@link GenericRecord}s of a given
	 * schema and looks up writer schema in Schema Registry.
	 *
	 * @param valueSchema reader schema
	 * @return A new schema builder
	 */
	public static Builder<Void, GenericRecord, GenericRecord> builder(Schema valueSchema) {
		return new Builder<>(GenericRecord.class, valueSchema);
	}

	@Override
	@SuppressWarnings("unchecked")
	public O deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
		Tuple2<KafkaAvroDeserializer, KafkaAvroDeserializer> deserializer = getDeserializer(consumerRecord.topic());
		K key = deserializer.f0 == null ? null : (K) deserializer.f0.deserialize(consumerRecord.topic(), consumerRecord.headers(), consumerRecord.key());
		V value = (V) deserializer.f1.deserialize(consumerRecord.topic(), consumerRecord.headers(), consumerRecord.value());
		return createOutput(key, value);
	}

	private void checkInitialized() {
		if (deserializers != null) {
			return;
		}

		deserializers = new HashMap<>();

		if (registry == null) {
			registry = new SchemaRegistryClient(baseConfig);
		}
	}

	@VisibleForTesting
	protected void setRegistryClient(SchemaRegistryClient registry) {
		this.registry = registry;
	}

	private Tuple2<KafkaAvroDeserializer, KafkaAvroDeserializer> getDeserializer(String topic) throws Exception {
		checkInitialized();
		Tuple2<KafkaAvroDeserializer, KafkaAvroDeserializer> deserializer = deserializers.get(topic);

		if (deserializer == null) {
			KafkaAvroDeserializer keyDeserializer = keyReaderSchema == null ? null : createDeserializer(topic, keyReaderSchema, true);
			KafkaAvroDeserializer valueDeserializer = createDeserializer(topic, valueReaderSchema, false);

			deserializer = Tuple2.of(keyDeserializer, valueDeserializer);
			deserializers.put(topic, deserializer);
		}

		return deserializer;
	}

	private KafkaAvroDeserializer createDeserializer(String topic, String schema, boolean isKey) throws Exception {
		Map<String, Object> config = new HashMap<>(baseConfig);

		KafkaAvroSerializer serializer = new KafkaAvroSerializer(registry);
		serializer.configure(config, isKey);

		String description = "Schema registered by Flink Kafka consumer for topic: [" + topic + "] iskey: [" + isKey + "]";
		SchemaMetadata metadata = new SchemaMetadata.Builder(serializer.getSchemaKey(topic, isKey))
			.description(description)
			.compatibility((SchemaCompatibility) config.get(KafkaAvroSerializer.SCHEMA_COMPATIBILITY))
			.build();

		SchemaIdVersion schemaIdVersion = registry.addSchemaVersion(metadata, new SchemaVersion(schema, description));
		config.put(KafkaAvroDeserializer.READER_VERSIONS, Collections.singletonMap(topic, schemaIdVersion.getVersion()));

		KafkaAvroDeserializer valueSerializer = new KafkaAvroDeserializer(registry);
		valueSerializer.configure(config, isKey);

		return valueSerializer;
	}

	protected abstract O createOutput(K key, V value);

	@Override
	public abstract TypeInformation<O> getProducedType();

	@Override
	public boolean isEndOfStream(O t) {
		return false;
	}

	private static Schema getSchema(Class<?> clazz) {
		SpecificData specificData = new SpecificData(Thread.currentThread().getContextClassLoader());
		return specificData.getSchema(clazz);
	}

	/**
	 * Builder class for {@link SchemaRegistryDeserializationSchema}.
	 */
	public static final class Builder<K, V, O> {

		private final Map<String, Object> config = new HashMap<>();
		private final Class<V> valueClazz;
		private final Schema valueSchema;

		private Class<K> keyClazz;
		private Schema keySchema;

		private Builder(Class<V> valueClazz, Schema valueSchema) {
			this.valueSchema = Preconditions.checkNotNull(valueSchema);
			this.valueClazz = valueClazz;
		}

		public Builder<K, V, O> setRegistryAddress(String schemaRegistryAddress) {
			config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryAddress);
			return this;
		}

		public Builder<K, V, O> setConfig(Map<String, ?> conf) {
			config.putAll(conf);
			return this;
		}

		public <X> Builder<X, V, Tuple2<X, V>> readKey(Class<X> keyClazz) {
			return readKey(keyClazz, getSchema(keyClazz));
		}

		public Builder<GenericRecord, V, Tuple2<GenericRecord, V>> readKey(Schema keySchema) {
			return readKey(GenericRecord.class, keySchema);
		}

		private <X> Builder<X, V, Tuple2<X, V>> readKey(Class<X> keyClazz, Schema keySchema) {
			this.keyClazz = (Class) keyClazz;
			this.keySchema = keySchema;
			return (Builder) this;
		}

		private void validate() {
			if (!config.containsKey(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name())) {
				throw new IllegalArgumentException("Schema Registry address must be set either using .setRegistryAddress(..) or through the config.");
			}
		}

		public SchemaRegistryDeserializationSchema<K, V, O> build() {
			validate();
			if (keyClazz == null) {
				return new NonKeyedRegistryDeserializationSchema(valueClazz, valueSchema, config);
			} else {
				return new KeyedRegistryDeserializationSchema(keyClazz, keySchema, valueClazz, valueSchema, config);
			}
		}
	}

	private static class NonKeyedRegistryDeserializationSchema<V> extends SchemaRegistryDeserializationSchema<Void, V, V> {

		private transient Class<V> valueClazz;
		private transient Schema valueSchema;

		public NonKeyedRegistryDeserializationSchema(Class<V> valueClazz, Schema valueSchema, Map<String, Object> baseConfig) {
			super(null, valueSchema, baseConfig);
			this.valueClazz = valueClazz;
			this.valueSchema = valueSchema;
		}

		@Override
		protected V createOutput(Void key, V value) {
			return value;
		}

		@Override
		public TypeInformation<V> getProducedType() {
			return getTypeInfo(valueClazz, valueSchema);
		}
	}

	private static class KeyedRegistryDeserializationSchema<K, V> extends SchemaRegistryDeserializationSchema<K, V, Tuple2<K, V>> {

		private transient Class<K> keyClazz;
		private transient Schema keySchema;

		private transient Class<V> valueClazz;
		private transient Schema valueSchema;

		public KeyedRegistryDeserializationSchema(Class<K> keyClazz, Schema keySchema, Class<V> valueClazz, Schema valueSchema, Map<String, Object> baseConfig) {
			super(Preconditions.checkNotNull(keySchema), valueSchema, baseConfig);
			this.keyClazz = Preconditions.checkNotNull(keyClazz);
			this.keySchema = keySchema;
			this.valueClazz = valueClazz;
			this.valueSchema = valueSchema;
		}

		@Override
		protected Tuple2<K, V> createOutput(K key, V value) {
			return Tuple2.of(key, value);
		}

		@Override
		public TypeInformation<Tuple2<K, V>> getProducedType() {
			return new TupleTypeInfo<>(getTypeInfo(keyClazz, keySchema), getTypeInfo(valueClazz, valueSchema));
		}
	}

	private static <T> TypeInformation<T> getTypeInfo(Class<T> clazz, Schema schema) {
		if (GenericRecord.class.equals(clazz)) {
			return (TypeInformation<T>) new GenericRecordAvroTypeInfo(schema);
		} else {
			return TypeInformation.of(clazz);
		}
	}

}

