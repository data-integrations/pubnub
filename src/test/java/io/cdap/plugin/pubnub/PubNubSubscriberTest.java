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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class PubNubSubscriberTest {
  private static final String MOCK_STAGE = "mockStage";

  private static final PubNubConfig VALID_CONFIG =
    new PubNubConfig("PubNub1", "pubnub_onboarding_channel", "subscriber_key",
                     0, 5, 120, "proxy-hostname.net",
                     5543, false, "linear", null, "socks",
                     true, "admin", "admin", "{}");

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidReferenceName() {
    PubNubConfig config = PubNubConfig.builder(VALID_CONFIG)
      .setReferenceName("!@#$%^&*(")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testInvalidReconnectionPolicy() {
    PubNubConfig config = PubNubConfig.builder(VALID_CONFIG)
      .setReconnectionPolicy("nonExisting")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, PubNubConfig.PROPERTY_RECONNECTION_POLICY);
  }

  @Test
  public void testInvalidProxyType() {
    PubNubConfig config = PubNubConfig.builder(VALID_CONFIG)
      .setProxyType("nonExisting")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, PubNubConfig.PROPERTY_PROXY_TYPE);
  }


  @Test
  public void testProxyTypeNoneAndProxyEnabled() {
    PubNubConfig config = PubNubConfig.builder(VALID_CONFIG)
      .setProxyType("None")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, PubNubConfig.PROPERTY_PROXY_TYPE);
  }

  @Test
  public void testNoProxyHostnameAndProxyEnabled() {
    PubNubConfig config = PubNubConfig.builder(VALID_CONFIG)
      .setProxyHostname(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, PubNubConfig.PROPERTY_PROXY_HOSTNAME);
  }

  @Test
  public void testNoProxyPortAndProxyEnabled() {
      PubNubConfig config = PubNubConfig.builder(VALID_CONFIG)
        .setProxyPort(null)
        .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    assertValidationFailed(failureCollector, PubNubConfig.PROPERTY_PROXY_PORT);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();

    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
