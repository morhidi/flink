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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.formats.avro.registry.cloudera.generated.TestRecord;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.util.InstantiationUtil;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for the {@link SchemaRegistryDeserializationSchema}.
 */
public class SchemaRegistryDeserializationSchemaTest extends SchemaRegistryTestBase {

	@Test
	public void testConfigValidation() {

		Schema schema1 = SchemaBuilder
			.record("TestRecord")
			.namespace("org.apache.flink.formats.avro.registry.cloudera")
			.fields()
			.optionalInt("intField")
			.optionalString("stringField")
			.endRecord();

		// Missing registry address
		try {
			SchemaRegistryDeserializationSchema
				.builder(schema1)
				.build();
		} catch (IllegalArgumentException expected) {}

		// Test address via conf
		SchemaRegistryDeserializationSchema
			.builder(Integer.class)
			.setConfig(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), "address"))
			.build();

		// Test address via conf
		SchemaRegistryDeserializationSchema
			.builder(Integer.class)
			.setRegistryAddress("address")
			.build();
	}

	@Test
	public void testGenericDeserialization() throws Exception {
		String topic1 = "t1";

		SchemaRegistrySerializationSchema<Void, TestRecord, TestRecord> ser = SchemaRegistrySerializationSchema
			.<TestRecord>builder(topic1)
			.setRegistryAddress("address")
			.build();

		ser.setRegistryClient(registry);
		ConsumerRecord<byte[], byte[]> consumerRecord = toConsumerRecord(ser.serialize(new TestRecord(1), System.currentTimeMillis()));

		Schema schema1 = SchemaBuilder
			.record("TestRecord")
			.namespace("org.apache.flink.formats.avro.registry.cloudera.generated")
			.fields()
			.optionalInt("intField")
			.optionalString("stringField")
			.endRecord();

		SchemaRegistryDeserializationSchema<Void, GenericRecord, GenericRecord> deserializer1 = SchemaRegistryDeserializationSchema
			.builder(schema1)
			.setRegistryAddress("address")
			.build();

		assertEquals(new GenericRecordAvroTypeInfo(schema1), deserializer1.getProducedType());
		deserializer1.setRegistryClient(registry);

		GenericRecord record = deserializer1.deserialize(consumerRecord);
		assertEquals(1, record.get("intField"));
		assertNull(record.get("stringField"));

		SchemaRegistryDeserializationSchema<Void, TestRecord, TestRecord> deserializer2 = SchemaRegistryDeserializationSchema
			.builder(TestRecord.class)
			.setRegistryAddress("address")
			.build();

		assertEquals(new AvroTypeInfo<>(TestRecord.class), deserializer2.getProducedType());
		deserializer2.setRegistryClient(registry);

		TypeSerializer<TestRecord> s = deserializer2.getProducedType().createSerializer(new ExecutionConfig());

		TestRecord record2 = InstantiationUtil.deserializeFromByteArray(s, InstantiationUtil.serializeToByteArray(s, deserializer2.deserialize(consumerRecord)));
		assertEquals(record2, new TestRecord(1));
	}

	@Test
	public void testKeyedDeserialization() throws Exception {
		String topic1 = "t1";

		SchemaRegistrySerializationSchema<Integer, TestRecord, TestRecord> ser = SchemaRegistrySerializationSchema
			.<TestRecord>builder(topic1)
			.setRegistryAddress("address")
			.setKey(TestRecord::getIntField)
			.build();

		ser.setRegistryClient(registry);
		ConsumerRecord<byte[], byte[]> consumerRecord = toConsumerRecord(ser.serialize(new TestRecord(1), System.currentTimeMillis()));

		SchemaRegistryDeserializationSchema<Integer, TestRecord, Tuple2<Integer, TestRecord>> deserializer = SchemaRegistryDeserializationSchema
			.builder(TestRecord.class)
			.readKey(Integer.class)
			.setRegistryAddress("address")
			.build();

		assertEquals(new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, new AvroTypeInfo<>(TestRecord.class)), deserializer.getProducedType());
		deserializer.setRegistryClient(registry);

		TypeSerializer<Tuple2<Integer, TestRecord>> s = deserializer.getProducedType().createSerializer(new ExecutionConfig());

		Tuple2<Integer, TestRecord> record2 = InstantiationUtil.deserializeFromByteArray(s, InstantiationUtil.serializeToByteArray(s, deserializer.deserialize(consumerRecord)));
		assertEquals(record2, Tuple2.of(1, new TestRecord(1)));
	}

	private static ConsumerRecord<byte[], byte[]> toConsumerRecord(ProducerRecord<byte[], byte[]> record) {
		return new ConsumerRecord<>(record.topic(), 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0L, 0, 2, record.key(), record.value(), record.headers());
	}
}
