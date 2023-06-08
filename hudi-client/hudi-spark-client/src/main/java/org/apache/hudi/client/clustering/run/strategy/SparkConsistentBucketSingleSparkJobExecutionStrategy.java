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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.execution.MultiFileSparkLazyInsertIterable;
import org.apache.hudi.execution.bulkinsert.RDDConsistentBucketPartitioner;
import org.apache.hudi.hms.HiveMetastoreFactory;
import org.apache.hudi.hms.PartitionSchemaReader;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SparkConsistentBucketSingleSparkJobExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends SingleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LogManager.getLogger(SparkConsistentBucketSingleSparkJobExecutionStrategy.class);

  public SparkConsistentBucketSingleSparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext,
                                                              HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public Iterator<List<WriteStatus>> performClusteringWithRecordsIterator(Iterator<HoodieRecord<T>> records, int numOutputGroups, String instantTime,
                                                                          Map<String, String> strategyParams, Schema schema, List<HoodieFileGroupId> fileGroupIdList,
                                                                          boolean preserveHoodieMetadata, TaskContextSupplier taskContextSupplier) {
    Map<String, WriteHandleFactory> bucketToWriteHandleFactoryMap = new HashMap<>();
    return null;
  }

  @Override
  public Iterator<List<WriteStatus>> performClusteringWithRecordsIterator(Iterator<HoodieRecord<T>> records, int numOutputGroups, String instantTime,
                                                                          Map<String, String> strategyParams, Schema schema, List<HoodieFileGroupId> fileGroupIdList,
                                                                          boolean preserveHoodieMetadata, TaskContextSupplier taskContextSupplier,
                                                                          ClusteringGroupInfo clusteringGroupInfo) {
    Properties props = getWriteConfig().getProps();
    String tableName = getTableName(fileGroupIdList);
    String partitionPath = getPartitionPath(fileGroupIdList);
    props.put(HoodieWriteConfig.PARTITION_TABLE_NAME.key(), tableName);
    props.put(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key(), Boolean.FALSE.toString());
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder().withProps(props).build();
    LOG.info("running clustering on " + tableName);

    HoodieClusteringGroup hoodieClusteringGroup = clusteringGroupInfo.getHoodieClusteringGroup();
    Map<String, String> extraMetadata = hoodieClusteringGroup.getExtraMetadata();
    HoodieTable hoodieTable = getHoodieTable();
    RDDConsistentBucketPartitioner<T> partitioner = new RDDConsistentBucketPartitioner<>(hoodieTable, strategyParams, preserveHoodieMetadata);
    try {
      List<ConsistentHashingNode> nodes = ConsistentHashingNode.fromJsonString(extraMetadata.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY));
      partitioner.addHashingChildrenNodes(extraMetadata.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_PARTITION_KEY), nodes);
    } catch (Exception e) {
      LOG.error("Failed to add hashing children nodes", e);
      throw new HoodieClusteringException("Failed to add hashing children nodes", e);
    }
    partitioner.setUpPartitioner(partitionPath);
    return new MultiFileSparkLazyInsertIterable<>(records, false, newConfig, instantTime, hoodieTable,
        hoodieTable.getTaskContextSupplier(), true, null, partitioner);
  }

  private String getTableName(List<HoodieFileGroupId> fileGroupIdList) {
    String partitionPath = getPartitionPath(fileGroupIdList);
    PartitionSchemaReader partitionSchemaReader = HiveMetastoreFactory.build(getWriteConfig().getMetastoreUris());
    String tableName = partitionSchemaReader.getTableName(partitionPath);
    return tableName;
  }

  private String getPartitionPath(List<HoodieFileGroupId> fileGroupIdList) {
    String partitionPath = "";
    for (HoodieFileGroupId hoodieFileGroupId : fileGroupIdList) {
      if (!hoodieFileGroupId.getPartitionPath().isEmpty()) {
        return hoodieFileGroupId.getPartitionPath();
      }
    }
    return partitionPath;
  }

}
