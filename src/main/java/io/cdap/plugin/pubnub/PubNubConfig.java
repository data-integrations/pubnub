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
import co.cask.hydrator.common.ReferencePluginConfig;

import java.util.Arrays;
import java.util.List;

/**
 * PubNub configuration.
 */
public final class PubNubConfig extends ReferencePluginConfig {
  @Name("channels")
  @Description("PubNub message are sent on a channel. Can subscribe to multiple channels.")
  private String channels;

  @Name("subscriber-key")
  @Description("A subscriber key to read data from the channels subscribed.")
  private String subscriberKey;

  @Name("connection-timeout")
  @Description("")
  private int connectionTimeout;

  @Name("filter-expression")
  @Description("")
  private String  filterExpression;

  @Name("proxy-hostname")
  @Description("")
  private String proxyHostname;

  @Name("proxy-port")
  @Description("")
  private int proxyPort;

  public PubNubConfig(String referenceName) {
    super(referenceName);
  }

  public List<String> getChannels() {
    return Arrays.asList(channels.split(","));
  }

  public String getSubscriberKey() {
    return subscriberKey;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public String getFilterExpression() {
    return  filterExpression;
  }

  public String getProxyHostname() {
    return proxyHostname;
  }

  public int getProxyPort() {
    return proxyPort;
  }
}
