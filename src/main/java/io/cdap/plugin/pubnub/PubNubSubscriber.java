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

import com.pubnub.api.PNConfiguration;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.Constants;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.pubnub.PubNubUtils;
import org.apache.spark.streaming.pubnub.SparkPubNubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * A PubNub subscriber for reading real-time messages.
 *
 * <p>This class <code>PubNubSubscriber</code> leverages cloud infrastructure of PubNub. The class
 * defines a pre-defined schema containing channel, publisher, payload, subscription and timetoken.</p>
 *
 * <p>Uses the <code>PNConfiguration</code> to configure the PubNub using the Java client. It sets
 * important configuration like connection timeout, reconnect attempts, proxy setting, and also authorization
 * key.</p>
 *
 * @see PNConfiguration
 * @see PubNubUtils
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("PubNubSubscriber")
@Description("Reads realtime messages from PubNub cloud.")
public final class PubNubSubscriber extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PubNubSubscriber.class);
  private final PubNubConfig config;
  private static final String CHANNEL = "channel";
  private static final String TIMETOKEN = "timetoken";
  private static final String PUBLISHER = "publisher";
  private static final String PAYLOAD = "payload";
  private static final String SUBSCRIPTION = "subscription";

  /**
   * Defines the pre-defined output schema for the PubNub subscriber.
   */
  public static final Schema SUBSCRIBER_SCHEMA = Schema.recordOf("subscriber",
    Schema.Field.of(CHANNEL, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of(TIMETOKEN, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(PUBLISHER, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of(PAYLOAD, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of(SUBSCRIPTION, Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );

  /**
   * Constructor to initialize <code>PubNubSubscriber</code>.
   *
   * @param conf a <code>PubNubConfig</code> specifying the configuration of plugin.
   */
  public PubNubSubscriber(PubNubConfig conf) {
    this.config = conf;
  }

  /**
   * Configures the <code>PubNubSubscriber</code>.
   * This methods registers a external PubNub Dataset as well as sets the output schema of the plugin.
   *
   * @param configurer a <code>PipelineConfigurer</code> for configuring the plugin during deployment phase.
   * @throws IllegalArgumentException thrown when there are issues with the configurations.
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);

    FailureCollector failureCollector = configurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    configurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    configurer.getStageConfigurer().setOutputSchema(SUBSCRIBER_SCHEMA);
  }

  /**
   * Reads the events from PubNub using the Java receiver.
   *
   * @param context a <code>StreamingContext</code> for accessing Spark streaming context.
   * @return a <code>JavaDStream</code> instance.
   * @throws Exception throw when there is issue configuring or retriveing events from PubNub.
   */
  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    context.registerLineage(config.referenceName);

    PNConfiguration pnConfig = new PNConfiguration();
    pnConfig.setSubscribeKey(config.getSubscriberKey());
    pnConfig.setSecure(config.isSecure());
    pnConfig.setSubscribeTimeout(config.getSubscribeTimeout());
    pnConfig.setReconnectionPolicy(config.getReconnectionPolicy());
    pnConfig.setConnectTimeout(config.getConnectionTimeout());
    pnConfig.setMaximumReconnectionRetries(config.getMaxReconnectAttempts());

    if (config.getCipherKey() != null) {
      pnConfig.setCipherKey(config.getCipherKey());
    }

    if (config.hasProxy()) {
      if (config.getProxyType() != null) {
        throw new IllegalArgumentException("Proxy enabled, but proxy type is specified as 'None'");
      }

      if (config.getProxyHostname() != null && config.getProxyPort() != null) {
        SocketAddress proxyAddress = new InetSocketAddress(config.getProxyHostname(), config.getProxyPort());
        Proxy proxy = new Proxy(config.getProxyType(), proxyAddress);
        pnConfig.setProxy(proxy);
        if (config.getProxyUsername() != null && config.getProxyPassword() != null) {
          pnConfig.setProxyAuthenticator(new Authenticator() {
            @Nullable
            @Override
            public Request authenticate(Route route, Response response) throws IOException {
              if (response.request().header("Authorization") != null) {
                return null; // Give up, we've already failed to authenticate.
              }
              String credential = Credentials.basic(config.getProxyUsername(), config.getProxyPassword());
              return response.request().newBuilder()
                .header("Authorization", credential)
                .build();
            }
          });
        }
      } else {
        throw new IllegalArgumentException("Proxy enabled, but hostname or port not specified.");
      }
    }

    JavaDStream<StructuredRecord> stream = PubNubUtils.createStream(
      context.getSparkStreamingContext(), pnConfig,
      config.getChannels(), Collections.EMPTY_SET, Option.empty(),
      StorageLevel.MEMORY_AND_DISK_SER_2()
    ).map(
      new Function<SparkPubNubMessage, StructuredRecord>() {
        @Override
        public StructuredRecord call(SparkPubNubMessage message) throws Exception {
          String channel = message.getChannel();
          StructuredRecord.Builder builder = StructuredRecord.builder(SUBSCRIBER_SCHEMA);
          if (channel != null) {
            builder.set(CHANNEL, channel);
          }
          builder.set(SUBSCRIPTION, message.getSubscription());
          builder.set(TIMETOKEN, message.getTimestamp());
          builder.set(PUBLISHER, message.getPublisher());
          builder.set(PAYLOAD, message.getPayload());
          StructuredRecord record = builder.build();
          return record;
        }
      }
    );

    return stream;
  }
}
