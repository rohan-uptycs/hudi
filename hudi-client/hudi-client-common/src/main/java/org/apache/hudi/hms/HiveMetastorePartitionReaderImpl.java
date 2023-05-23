/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hms;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;

public class HiveMetastorePartitionReaderImpl implements PartitionSchemaReader, Serializable {

  private transient HiveMetaStoreClient hiveMetaStoreClient;
  private HiveConf conf;
  private Cache<String, Schema> schemaCache;

  private static Map<String, Schema.Type> schemaMapping = Collections.unmodifiableMap(new HashMap<String, Schema.Type>() {
    {
      put("string", Schema.Type.STRING);
      put("int", Schema.Type.INT);
      put("long", Schema.Type.LONG);
      put("bigint", Schema.Type.LONG);
      put("map<string,string>", Schema.Type.MAP);
      put("double", Schema.Type.DOUBLE);
      put("boolean", Schema.Type.BOOLEAN);
    }
  });

  protected HiveMetastorePartitionReaderImpl(String metastoreUris) {
    hiveMetaStoreClient = initClient(metastoreUris);
    schemaCache = Caffeine.newBuilder().maximumSize(10000).build();
  }

  private HiveMetaStoreClient initClient(String metastoreUris) {
    List<String> metastores = Arrays.asList(metastoreUris.split(","));
    Collections.shuffle(metastores);
    conf = new HiveConf();
    conf.setVar(
        HiveConf.ConfVars.METASTOREURIS, metastores.stream().collect(Collectors.joining(",")));
    conf.setVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, "10");
    conf.setVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, "5");
    try {
      return new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Schema getPartitionSchema(String tableName, Schema oldWriterSchema) {
    String key = tableName;
    Schema schema = schemaCache.get(key, new Function<String, Schema>() {
      @Override
      public Schema apply(String s) {
        return getAvroSchema(tableName, oldWriterSchema);
      }
    });
    return schema;
  }

  private Schema getAvroSchema(String tableName, Schema writerSchema) {
    List<FieldSchema> fieldSchemaList = getSchema(tableName);
    Set<Schema.Field> schemaFields = new LinkedHashSet<>();
    List<Schema.Field> schemaFieldsList = new ArrayList<>();

    for (Schema.Field field : writerSchema.getFields()) {
      Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
      for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
        newField.addProp(prop.getKey(), prop.getValue());
      }
      schemaFields.add(newField);
    }
    fieldSchemaList.forEach(field -> {
      if (!field.getName().contains("upt_")) {
        if (!field.getType().toLowerCase().contains("map<")) {
          Schema.Type type = getAvrotType(field.getType().toLowerCase());
          schemaFields.add(new Schema.Field(field.getName(), createNullableSchema(Schema.create(type)), "", JsonProperties.NULL_VALUE));
        }
      }
    });
    Schema mergedSchema = Schema.createRecord(writerSchema.getName(), writerSchema.getDoc(), writerSchema.getNamespace(), false);
    schemaFieldsList.addAll(schemaFields);
    mergedSchema.setFields(schemaFieldsList);
    return mergedSchema;
  }

  private List<FieldSchema> getSchema(String tablename) {
    String key = tablename;
    synchronized (this) {
      List<FieldSchema> fieldSchemas = null;
      try {
        fieldSchemas = hiveMetaStoreClient.getSchema("upt_system", tablename);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return fieldSchemas;
    }
  }

  private Schema.Type getAvrotType(String type) {
    if (schemaMapping.containsKey(type)) {
      return schemaMapping.get(type);
    }
    return Schema.Type.STRING;
  }
}
