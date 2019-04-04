/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.pubnub;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.IdUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PubNub subscriber.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("PubNubSubscriber")
@Description("A PubNub channel subscriber")
public final class PubNubSubscriber extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PubNubSubscriber.class);
  private PubNubConfig config;

  public PubNubSubscriber(PubNubConfig conf) {
    this.config = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    IdUtils.validateId(config.referenceName);
    configurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    configurer.getStageConfigurer().setOutputSchema(PubNubReceiver.SUBSCRIBER_SCHEMA);
  }


  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    JavaReceiverInputDStream<StructuredRecord> streamer =
      context.getSparkStreamingContext().receiverStream(new PubNubReceiver(config, context));
    return streamer;
  }
}
