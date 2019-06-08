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
import com.pubnub.api.enums.PNReconnectionPolicy;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
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
@Description("A PubNub channel subscriber")
public final class PubNubSubscriber extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PubNubSubscriber.class);
  private final PubNubConfig config;
  private static final String CHANNEL = "channel";
  private static final String TIMETOKEN = "timetoken";
  private static final String PUBLISHER = "publisher";
  private static final String PAYLOAD = "payload";
  private static final String SUBSCRIPTION = "subscription";
  private static final int MAX_RECONNECT_ATTEMPTS = 5;
  private static final int CONNECT_TIMEOUT_IN_SECONDS = 5;

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
    IdUtils.validateId(config.referenceName);
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
    pnConfig.setConnectTimeout(config.getConnectionTimeout());
    pnConfig.setReconnectionPolicy(config.getReconnectionPolicy());
    pnConfig.setMaximumReconnectionRetries(config.getMaxReconnectAttempts());

    if (config.hasProxy()) {
      if (config.getProxyHostname() != null && config.getProxyPort() != null) {
        SocketAddress proxyAddress = new InetSocketAddress(config.getProxyHostname(), config.getProxyPort());
        pnConfig.setProxy(new Proxy(config.getProxyType(), proxyAddress));
      } else {
        throw new IllegalArgumentException("Proxy enabled, but hostname or port not specified.");
      }
    }

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
    @Description("Maximum number of seconds which the client should wait for connection before timing out")
    @Nullable
    private Integer connectionTimeout;

    @Name("max-reconnect-attempts")
    @Description("Set how many times the reconneciton manager will try to connect before giving up")
    @Nullable
    private Integer maxReconnectAttempts;

    @Name("proxy-hostname")
    @Description("Proxy hostname")
    @Nullable
    private String proxyHostname;

    @Name("proxy-port")
    @Description("Proxy port")
    @Nullable
    private Integer proxyPort;

    @Name("secure")
    @Description("Switch the client to HTTPS based communications")
    @Nullable
    private Boolean secure;

    @Name("reconnection-policy")
    @Description("Reconnection policy which will be used if/when networking goes down")
    @Nullable
    private String reconnectionPolicy;

    @Name("auth-key")
    @Description("Authorizaton key")
    @Nullable
    private String authKey;

    @Name("proxy-type")
    @Description("Specify the type of proxy")
    @Nullable
    private String proxyType;

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
      this.maxReconnectAttempts = 5;
    }

    /**
     * @return a <code>Set</code> of channels to be connected using the subscription key.
     */
    public Set<String> getChannels() {
      return Sets.newHashSet(Arrays.asList(channels.split(",")));
    }

    /**
     * @return a <code>String</code> specifying the subscriber key used to connect to PubNub.
     */
    public String getSubscriberKey() {
      return subscriberKey;
    }

    /**
     * @return a <code>Integer</code> specifying the connection timeout in seconds.
     */
    public int getConnectionTimeout() {
      if (connectionTimeout != null) {
        return connectionTimeout;
      }
      return CONNECT_TIMEOUT_IN_SECONDS;
    }

    /**
     * @return a <code>int</code> specifying the max reconnect attempts before giving up.
     */
    public int getMaxReconnectAttempts() {
      if (maxReconnectAttempts != null) {
        return maxReconnectAttempts;
      }
      return MAX_RECONNECT_ATTEMPTS;
    }

    public Proxy.Type getProxyType() {
      if (proxyType.equalsIgnoreCase("http")) {
        return Proxy.Type.HTTP;
      }

      if (proxyType.equalsIgnoreCase("direct")) {
        return Proxy.Type.DIRECT;
      }

      if (proxyType.equalsIgnoreCase("socks")) {
        return Proxy.Type.SOCKS;
      }

      return Proxy.Type.HTTP;
    }

    /**
     * @return a <code>Boolean</code> type specifying whether proxy is required.
     */
    @Nullable
    public boolean hasProxy() {
      return useProxy;
    }

    /**
     * @return a <code>String</code> specifying the hostname of the proxy to be used to connect.
     */
    public String getProxyHostname() {
      return proxyHostname;
    }

    /**
     * @return a <code>Integer</code> specifying the proxy port.
     */
    public Integer getProxyPort() {
      return proxyPort;
    }

    /**
     * @return a instance of <code>PNReconnectPolicy</code> specifying how the client should backoff before reconnect.
     */
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

    /**
     * @return a <code>Boolean</code> type specifying whether to use secure client connection to connect to PubNub.
     */
    @Nullable
    public boolean isSecure() {
      return secure;
    }

    /**
     * @return a <code>String</code> type specifying the authorization key for PubNub.
     */
    @Nullable
    public String getAuthKey() {
      return authKey;
    }
  }

}
