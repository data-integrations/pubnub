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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A receiver for consuming data
 */
public final class PubNubReceiver extends Receiver<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PubNubReceiver.class);
  private final PubNubConfig config;
  private final PNConfiguration pnConfiguration;
  private final PubNub pubnub;
  private static final long RECONNECT_TIMEOUT_IN_MS = 200;
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

  public PubNubReceiver(PubNubConfig config, StreamingContext context) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.config = config;

    pnConfiguration = new PNConfiguration();
    pnConfiguration.setSubscribeKey(config.getSubscriberKey());
    pnConfiguration.setConnectTimeout(config.getConnectionTimeout());

    pubnub = new PubNub(pnConfiguration);
  }

  @Override
  public void onStart() {
    if (pubnub != null) {
      pubnub.addListener(new SubscribeCallback() {
        @Override
        public void status(PubNub pubNub, PNStatus status) {
          if (status.getCategory() == PNStatusCategory.PNUnexpectedDisconnectCategory) {
            pubnub.reconnect();
          } else if (status.getCategory() == PNStatusCategory.PNTimeoutCategory) {
            try {
              Thread.sleep(RECONNECT_TIMEOUT_IN_MS);
            } catch (InterruptedException e) {
              // nothing to do here.
            }
            pubnub.reconnect();
          }
        }

        @Override
        public void message(PubNub pubNub, PNMessageResult message) {
          String channel = message.getChannel();
          StructuredRecord.Builder builder = StructuredRecord.builder(SUBSCRIBER_SCHEMA);
          if (channel != null) {
            builder.set(CHANNEL, channel);
          }
          builder.set(TIMETOKEN, message.getTimetoken());
          builder.set(PUBLISHER, message.getPublisher());
          builder.set(PAYLOAD, message.getMessage().getAsString());
          StructuredRecord record = builder.build();
          store(record);
        }

        @Override
        public void presence(PubNub pubNub, PNPresenceEventResult presence) {

        }
      });

      pubnub.subscribe()
        .channels(config.getChannels())
        .execute();
    }
  }

  @Override
  public void onStop() {
    if (pubnub != null) {
      pubnub.unsubscribe()
        .channels(config.getChannels())
        .execute();
      pubnub.destroy();
    }
  }
}
