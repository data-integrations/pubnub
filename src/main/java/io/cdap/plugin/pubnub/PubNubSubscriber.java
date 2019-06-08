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

import com.google.common.collect.Sets;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.enums.PNLogVerbosity;
import com.pubnub.api.enums.PNReconnectionPolicy;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.pubnub.PubNubUtils;
import org.apache.spark.streaming.pubnub.SparkPubNubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * PubNub subscriber.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("PubNubSubscriber")
@Description("A PubNub channel subscriber")
public final class PubNubSubscriber extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PubNubSubscriber.class);
  private final PubNubConfig config;
  private static final String CHANNEL = "channel";
  private static final String TIMETOKEN = "timetoken";
  private static final String PUBLISHER = "publisher";
  private static final String PAYLOAD = "payload";

  public static final Schema SUBSCRIBER_SCHEMA = Schema.recordOf("subscriber",
    Schema.Field.of(CHANNEL, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of(TIMETOKEN, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(PUBLISHER, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of(PAYLOAD, Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );

  public PubNubSubscriber(PubNubConfig conf) {
    this.config = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    configurer.getStageConfigurer().setOutputSchema(SUBSCRIBER_SCHEMA);
  }


  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    context.registerLineage(config.referenceName);


    PNConfiguration pnConfig = new PNConfiguration();
    pnConfig.setSubscribeKey(config.getSubscriberKey());
    pnConfig.setSecure(config.isSecure());
    if (config.getConnectionTimeout() != -1) {
      pnConfig.setConnectTimeout(config.getConnectionTimeout());
    }
    pnConfig.setReconnectionPolicy(config.getReconnectionPolicy());

    if (config.hasProxy()) {
      SocketAddress proxyAddress = new InetSocketAddress(config.getProxyHostname(), config.getProxyPort());
      pnConfig.setProxy(new Proxy(Proxy.Type.HTTP, proxyAddress));
    }

    pnConfig.setLogVerbosity(PNLogVerbosity.NONE);
    if (config.getAuthKey() != null) {
      pnConfig.setAuthKey(config.getAuthKey());
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

  /**
   * PubNub configuration.
   */
  public static class PubNubConfig extends ReferencePluginConfig implements Serializable {

    private static final long serialVersionUID = 4219063781909515444L;

    @Name("channels")
    @Description("PubNub message are sent on a channel. Can subscribe to multiple channels.")
    private String channels;

    @Name("subscriber-key")
    @Description("A subscriber key to read data from the channels subscribed.")
    private String subscriberKey;

    @Name("connection-timeout")
    @Description("Connection timeout")
    @Nullable
    private Integer connectionTimeout;

    @Name("proxy-hostname")
    @Description("Proxy hostname")
    @Nullable
    private String proxyHostname;

    @Name("proxy-port")
    @Description("Proxy port")
    @Nullable
    private Integer proxyPort;

    @Name("secure")
    @Description("Connect using HTTPS")
    @Nullable
    private Boolean secure;

    @Name("reconnection-policy")
    @Description("Connect using HTTPS")
    @Nullable
    private String reconnectionPolicy;

    @Name("auth-key")
    @Description("Authorizaton key")
    @Nullable
    private String authKey;

    @Name("use-proxy")
    @Description("Specify a proxy to be used to connect to PubNub")
    @Nullable
    private Boolean useProxy;

    @Name("schema")
    @Description("Defines the output schema")
    private String schema;


    public PubNubConfig(String referenceName) {
      super(referenceName);
      this.secure = false;
      this.reconnectionPolicy = "linear";
      this.connectionTimeout = -1;
      this.useProxy = false;
    }

    public Set<String> getChannels() {
      return Sets.newHashSet(Arrays.asList(channels.split(",")));
    }

    public String getSubscriberKey() {
      return subscriberKey;
    }

    public int getConnectionTimeout() {
      return connectionTimeout;
    }

    public String getProxyHostname() {
      return proxyHostname;
    }

    public int getProxyPort() {
      return proxyPort;
    }

    public PNReconnectionPolicy getReconnectionPolicy() {
      if (reconnectionPolicy.equalsIgnoreCase("none")) {
        return PNReconnectionPolicy.NONE;
      }

      if (reconnectionPolicy.equalsIgnoreCase("linear")) {
        return PNReconnectionPolicy.LINEAR;
      }

      if (reconnectionPolicy.equalsIgnoreCase("exponential")) {
        return  PNReconnectionPolicy.EXPONENTIAL;
      }

      return PNReconnectionPolicy.LINEAR;

    }

    public boolean isSecure() {
      return secure;
    }

    @Nullable
    public boolean hasProxy() {
      return useProxy;
    }

    public String getAuthKey() {
      return authKey;
    }
  }

}
