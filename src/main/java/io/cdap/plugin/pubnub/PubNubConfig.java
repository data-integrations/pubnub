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
import com.pubnub.api.enums.PNReconnectionPolicy;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.io.Serializable;
import java.net.Proxy;
import java.util.Arrays;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Configuration for {@link PubNubSubscriber}
 */
public class PubNubConfig extends ReferencePluginConfig implements Serializable {
  public static final String PROPERTY_RECONNECTION_POLICY = "reconnection-policy";
  public static final String PROPERTY_USE_PROXY = "use-proxy";
  public static final String PROPERTY_PROXY_TYPE = "proxy-type";
  public static final String PROPERTY_PROXY_HOSTNAME = "proxy-hostname";
  public static final String PROPERTY_PROXY_PORT = "proxy-port";

  private static final long serialVersionUID = 4219063781909515444L;

  private static final int MAX_RECONNECT_ATTEMPTS = 5;
  private static final int CONNECT_TIMEOUT_IN_SECONDS = 5;

  @Name("channels")
  @Description("PubNub message are sent on a channel. Can subscribe to multiple channels.")
  private final String channels;

  @Name("subscriber-key")
  @Description("A subscriber key to read data from the channels subscribed.")
  private final String subscriberKey;

  @Name("connection-timeout")
  @Description("Maximum number of seconds which the client should wait for connection before timing out")
  @Nullable
  private final Integer connectionTimeout;

  @Name("max-reconnect-attempts")
  @Description("Set how many times the reconneciton manager will try to connect before giving up")
  @Nullable
  private final Integer maxReconnectAttempts;

  @Name("subscribe-timeout")
  @Description("Subscribe request timeout. Defaults to 310 seconds")
  @Nullable
  private final Integer subscribeTimeout;

  @Name(PROPERTY_PROXY_HOSTNAME)
  @Description("Proxy hostname")
  @Nullable
  private final String proxyHostname;

  @Name(PROPERTY_PROXY_PORT)
  @Description("Proxy port")
  @Nullable
  private final Integer proxyPort;

  @Name("secure")
  @Description("Switch the client to HTTPS based communications")
  @Nullable
  private final Boolean secure;

  @Name(PROPERTY_RECONNECTION_POLICY)
  @Description("Reconnection policy which will be used if/when networking goes down")
  @Nullable
  private final String reconnectionPolicy;

  @Name("cipher-key")
  @Description("Cipher for encrypting communications to/from PubNub will be encrypted.")
  @Nullable
  private final String cipher;

  @Name(PROPERTY_PROXY_TYPE)
  @Description("Specify the type of proxy")
  @Nullable
  private final String proxyType;

  @Name(PROPERTY_USE_PROXY)
  @Description("Specify a proxy to be used to connect to PubNub")
  @Nullable
  private final Boolean useProxy;

  @Name("proxy-username")
  @Description("Proxy Username")
  @Nullable
  private final String proxyUsername;

  @Name("proxy-password")
  @Description("Proxy Password")
  @Nullable
  private final String proxyPassword;

  @Name("schema")
  @Description("Defines the output schema")
  private final String schema;

  public PubNubConfig(String referenceName, String channels, String subscriberKey, @Nullable Integer connectionTimeout,
                      @Nullable Integer maxReconnectAttempts, @Nullable Integer subscribeTimeout,
                      @Nullable String proxyHostname, @Nullable Integer proxyPort, @Nullable Boolean secure,
                      @Nullable String reconnectionPolicy, @Nullable String cipher, @Nullable String proxyType,
                      @Nullable Boolean useProxy, @Nullable String proxyUsername, @Nullable String proxyPassword,
                      String schema) {
    super(referenceName);
    this.channels = channels;
    this.subscriberKey = subscriberKey;
    this.connectionTimeout = connectionTimeout;
    this.maxReconnectAttempts = maxReconnectAttempts;
    this.subscribeTimeout = subscribeTimeout;
    this.proxyHostname = proxyHostname;
    this.proxyPort = proxyPort;
    this.secure = secure;
    this.reconnectionPolicy = reconnectionPolicy;
    this.cipher = cipher;
    this.proxyType = proxyType;
    this.useProxy = useProxy;
    this.proxyUsername = proxyUsername;
    this.proxyPassword = proxyPassword;
    this.schema = schema;
  }

  private PubNubConfig(Builder builder) {
    super(builder.referenceName);
    this.channels = builder.channels;
    this.subscriberKey = builder.subscriberKey;
    this.connectionTimeout = builder.connectionTimeout;
    this.maxReconnectAttempts = builder.maxReconnectAttempts;
    this.subscribeTimeout = builder.subscribeTimeout;
    this.proxyHostname = builder.proxyHostname;
    this.proxyPort = builder.proxyPort;
    this.secure = builder.secure;
    this.reconnectionPolicy = builder.reconnectionPolicy;
    this.cipher = builder.cipher;
    this.proxyType = builder.proxyType;
    this.useProxy = builder.useProxy;
    this.proxyUsername = builder.proxyUsername;
    this.proxyPassword = builder.proxyPassword;
    this.schema = builder.schema;
  }

  public String getChannelsString() {
    return channels;
  }

  public String getSchemaString() {
    return schema;
  }

  public String getReconnectionPolicyString() {
    return reconnectionPolicy;
  }

  public String getProxyTypeString() {
    return proxyType;
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

  @Nullable
  public Proxy.Type getProxyType() {
    if (proxyType.equalsIgnoreCase("http")) {
      return Proxy.Type.HTTP;
    } else if (proxyType.equalsIgnoreCase("direct")) {
      return Proxy.Type.DIRECT;
    } else if (proxyType.equalsIgnoreCase("socks")) {
      return Proxy.Type.SOCKS;
    } else if (proxyType.equalsIgnoreCase("none")) {
      return null;
    }

    throw new IllegalStateException(String.format("Unsupported value for proxyType: '%s'",
                                                  proxyType));
  }

  /**
   * @return a <code>String</code> type representing a cipher.
   */
  @Nullable
  public String getCipherKey() {
    return cipher;
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
    } else if (reconnectionPolicy.equalsIgnoreCase("linear")) {
      return PNReconnectionPolicy.LINEAR;
    } else if (reconnectionPolicy.equalsIgnoreCase("exponential")) {
      return PNReconnectionPolicy.EXPONENTIAL;
    }

    throw new IllegalStateException(String.format("Unsupported value for reconnectionPolicy: '%s'",
                                                  reconnectionPolicy));
  }

  /**
   * @return a <code>Boolean</code> type specifying whether to use secure client connection to connect to PubNub.
   */
  @Nullable
  public boolean isSecure() {
    return secure;
  }

  /**
   * @return a <code>Integer</code> type specifying subscribe timeout.
   */
  @Nullable
  public Integer getSubscribeTimeout() {
    return subscribeTimeout;
  }

  /**
   * @return a <code>String</code> type specifying proxy username.
   */
  @Nullable
  public String getProxyUsername() {
    return proxyUsername;
  }

  /**
   * @return a <code>String</code> type specifying proxy password.
   */
  @Nullable
  public String getProxyPassword() {
    return proxyPassword;
  }

  public void validate(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(referenceName, failureCollector);

    try {
      getReconnectionPolicy();
    } catch (IllegalStateException ex) {
      failureCollector.addFailure(ex.getMessage(), null)
        .withConfigProperty(PROPERTY_RECONNECTION_POLICY);
    }

    if (hasProxy()) {
      try {
        if (getProxyType() == null) {
          failureCollector.addFailure("Proxy enabled, but proxy type is specified as 'None'", null)
            .withConfigProperty(PROPERTY_PROXY_TYPE);
        }
      } catch (IllegalStateException ex) {
        failureCollector.addFailure(ex.getMessage(), null)
          .withConfigProperty(PROPERTY_PROXY_TYPE);
      }

      if (getProxyHostname() == null) {
        failureCollector.addFailure("Proxy enabled, but hostname is not specified.", null)
          .withConfigProperty(PROPERTY_PROXY_HOSTNAME);
      }

      if (getProxyPort() == null) {
        failureCollector.addFailure("Proxy enabled, but port is not specified.", null)
          .withConfigProperty(PROPERTY_PROXY_PORT);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(PubNubConfig copy) {
    return new Builder()
      .setReferenceName(copy.referenceName)
      .setChannels(copy.getChannelsString())
      .setSubscriberKey(copy.getSubscriberKey())
      .setConnectionTimeout(copy.getConnectionTimeout())
      .setMaxReconnectAttempts(copy.getMaxReconnectAttempts())
      .setSubscribeTimeout(copy.getSubscribeTimeout())
      .setProxyHostname(copy.getProxyHostname())
      .setProxyPort(copy.getProxyPort())
      .setSecure(copy.isSecure())
      .setReconnectionPolicy(copy.getReconnectionPolicyString())
      .setCipher(copy.getCipherKey())
      .setProxyType(copy.getProxyTypeString())
      .setUseProxy(copy.hasProxy())
      .setProxyUsername(copy.getProxyUsername())
      .setProxyPassword(copy.getProxyPassword())
      .setSchema(copy.getSchemaString());
  }

  /**
   * Builder for {@PubNubConfig}
   */
  public static final class Builder {
    private String referenceName;
    private String channels;
    private String subscriberKey;
    private Integer connectionTimeout;
    private Integer maxReconnectAttempts;
    private Integer subscribeTimeout;
    private String proxyHostname;
    private Integer proxyPort;
    private Boolean secure;
    private String reconnectionPolicy;
    private String cipher;
    private String proxyType;
    private Boolean useProxy;
    private String proxyUsername;
    private String proxyPassword;
    private String schema;

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setChannels(String channels) {
      this.channels = channels;
      return this;
    }

    public Builder setSubscriberKey(String subscriberKey) {
      this.subscriberKey = subscriberKey;
      return this;
    }

    public Builder setConnectionTimeout(Integer connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
    }

    public Builder setMaxReconnectAttempts(Integer maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
      return this;
    }

    public Builder setSubscribeTimeout(Integer subscribeTimeout) {
      this.subscribeTimeout = subscribeTimeout;
      return this;
    }

    public Builder setProxyHostname(String proxyHostname) {
      this.proxyHostname = proxyHostname;
      return this;
    }

    public Builder setProxyPort(Integer proxyPort) {
      this.proxyPort = proxyPort;
      return this;
    }

    public Builder setSecure(Boolean secure) {
      this.secure = secure;
      return this;
    }

    public Builder setReconnectionPolicy(String reconnectionPolicy) {
      this.reconnectionPolicy = reconnectionPolicy;
      return this;
    }

    public Builder setCipher(String cipher) {
      this.cipher = cipher;
      return this;
    }

    public Builder setProxyType(String proxyType) {
      this.proxyType = proxyType;
      return this;
    }

    public Builder setUseProxy(Boolean useProxy) {
      this.useProxy = useProxy;
      return this;
    }

    public Builder setProxyUsername(String proxyUsername) {
      this.proxyUsername = proxyUsername;
      return this;
    }

    public Builder setProxyPassword(String proxyPassword) {
      this.proxyPassword = proxyPassword;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    private Builder() {
    }

    public PubNubConfig build() {
      return new PubNubConfig(this);
    }
  }
}
