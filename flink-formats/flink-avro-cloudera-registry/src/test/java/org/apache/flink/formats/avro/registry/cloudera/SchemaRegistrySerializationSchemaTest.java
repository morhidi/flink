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

import org.apache.flink.api.java.tuple.Tuple2;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerde;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for the {@link SchemaRegistrySerializationSchema}.
 */
public class SchemaRegistrySerializationSchemaTest extends SchemaRegistryTestBase {

	@Test
	public void testConfigValidation() {

		// Missing registry address
		try {
			SchemaRegistrySerializationSchema
				.builder("t")
				.build();
		} catch (IllegalArgumentException expected) {}

		// Test address via conf
		SchemaRegistrySerializationSchema
			.builder("t")
			.setConfig(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), "address"))
			.build();

		// Test address via conf
		SchemaRegistrySerializationSchema
			.builder("t")
			.setRegistryAddress("address")
			.build();
	}

	@Test
	public void testNonKeyedSerialization() {
		SchemaRegistrySerializationSchema<Void, Integer, Integer> schema = SchemaRegistrySerializationSchema
			.<Void, Integer>builder((k, v) -> Integer.toString(v % 2))
			.setRegistryAddress("address")
			.build();

		schema.setRegistryClient(registry);
		ProducerRecord<byte[], byte[]> record = schema.serialize(1, System.currentTimeMillis());

		assertNull(record.key());
		assertNotNull(record.value());
		assertEquals("1", record.topic());
		assertNull(record.partition());

		assertEquals(1, schemas.size());
		assertEquals(1, schemas.get("1").size());

		// Write some to the same topic
		schema.serialize(3, System.currentTimeMillis());
		schema.serialize(5, System.currentTimeMillis());
		schema.serialize(7, System.currentTimeMillis());

		assertEquals(1, schemas.size());
		assertEquals(1, schemas.get("1").size());

		// Write some to a new topic
		schema.serialize(2, System.currentTimeMillis());
		schema.serialize(4, System.currentTimeMillis());

		assertEquals(2, schemas.size());
		assertEquals(1, schemas.get("0").size());
		assertEquals(1, schemas.get("1").size());
	}

	@Test
	public void testKeyedSerialization() {
		String topic = "t";

		SchemaRegistrySerializationSchema<Integer, String, Tuple2<Integer, String>> schema = SchemaRegistrySerializationSchema
			.<String>builder(topic)
			.setRegistryAddress("address")
			.setConfig(Collections.singletonMap(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER, "true"))
			.<Integer>setKey()
			.build();

		schema.setRegistryClient(registry);
		ProducerRecord<byte[], byte[]> record = schema.serialize(Tuple2.of(1, "A"), null);

		assertNotNull(record.key());
		assertNotNull(record.value());
		assertEquals(topic, record.topic());
		assertNull(record.partition());

		Headers headers = record.headers();
		assertNotNull(headers.lastHeader(KafkaAvroSerde.DEFAULT_KEY_SCHEMA_VERSION_ID));
		assertNotNull(headers.lastHeader(KafkaAvroSerde.DEFAULT_VALUE_SCHEMA_VERSION_ID));
	}

}
