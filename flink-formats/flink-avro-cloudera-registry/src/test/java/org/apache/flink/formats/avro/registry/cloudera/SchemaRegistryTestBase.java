package org.apache.flink.formats.avro.registry.cloudera;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for testing {@link SchemaRegistryClient} based schemas.
 */
public class SchemaRegistryTestBase {

	protected Map<String, List<String>> schemas;
	protected Map<String, SchemaMetadataInfo> metas;
	protected Map<SchemaIdVersion, SchemaVersionInfo> versionInfos;
	protected Map<SchemaVersionKey, SchemaVersionInfo> versionInfos2;
	protected SchemaRegistryClient registry;

	private int nextId = 0;

	@Before
	public void setup() throws Exception {
		schemas = new HashMap<>();
		metas = new HashMap<>();
		versionInfos = new HashMap<>();
		versionInfos2 = new HashMap<>();
		registry = Mockito.mock(SchemaRegistryClient.class);

		Mockito
			.doAnswer(call -> addSchemaVersion(call.getArgument(0), call.getArgument(1)))
			.when(registry)
			.addSchemaVersion(Mockito.any(SchemaMetadata.class), Mockito.any(SchemaVersion.class));

		Mockito.doAnswer(c -> metas.get(c.getArgument(0))).when(registry).getSchemaMetadataInfo(Mockito.anyString());
		Mockito.doAnswer(c -> versionInfos.get(c.getArgument(0))).when(registry).getSchemaVersionInfo(Mockito.any(SchemaIdVersion.class));
		Mockito.doAnswer(c -> versionInfos2.get(c.getArgument(0))).when(registry).getSchemaVersionInfo(Mockito.any(SchemaVersionKey.class));
	}

	private SchemaIdVersion addSchemaVersion(SchemaMetadata metadata, SchemaVersion version) {
		String schemaText = version.getSchemaText();

		int v = schemas.compute(metadata.getName(), (name, s) -> {
			if (s == null) {
				s = new ArrayList<>();
			}

			if (!s.contains(schemaText)) {
				s.add(schemaText);
			}

			return s;
		}).indexOf(schemaText);

		SchemaIdVersion schemaIdVersion = new SchemaIdVersion((long) ++nextId, v, (long) ++nextId);
		SchemaVersionInfo versionInfo = new SchemaVersionInfo(schemaIdVersion.getSchemaMetadataId(), metadata.getName(), v, schemaIdVersion.getSchemaMetadataId(), schemaText, System.currentTimeMillis(), metadata.getDescription(), (byte) 0);

		versionInfos.put(schemaIdVersion, versionInfo);
		versionInfos2.put(new SchemaVersionKey(metadata.getName(), v), versionInfo);
		metas.put(metadata.getName(), new SchemaMetadataInfo(metadata, schemaIdVersion.getSchemaMetadataId(), System.currentTimeMillis()));

		return schemaIdVersion;
	}
}
