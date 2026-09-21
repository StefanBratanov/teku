/*
 * Copyright Consensys Software Inc., 2026
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package tech.pegasys.teku.validator.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tech.pegasys.teku.spec.SpecMilestone.GLOAS;
import static tech.pegasys.teku.spec.SpecMilestone.HEZE;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.ArgumentCaptor;
import tech.pegasys.infrastructure.logging.LogCaptor;
import tech.pegasys.teku.bls.BLSPublicKey;
import tech.pegasys.teku.ethereum.json.types.validator.ProposerDuties;
import tech.pegasys.teku.ethereum.json.types.validator.ProposerDuty;
import tech.pegasys.teku.infrastructure.async.SafeFuture;
import tech.pegasys.teku.infrastructure.logging.ValidatorLogger;
import tech.pegasys.teku.infrastructure.ssz.SszList;
import tech.pegasys.teku.infrastructure.unsigned.UInt64;
import tech.pegasys.teku.spec.Spec;
import tech.pegasys.teku.spec.TestSpecContext;
import tech.pegasys.teku.spec.TestSpecInvocationContextProvider.SpecContext;
import tech.pegasys.teku.spec.datastructures.builder.versions.gloas.BuilderConfig;
import tech.pegasys.teku.spec.datastructures.builder.versions.gloas.BuilderEntry;
import tech.pegasys.teku.spec.datastructures.builder.versions.gloas.BuilderPreferencesEntry;
import tech.pegasys.teku.spec.signatures.Signer;
import tech.pegasys.teku.spec.util.DataStructureUtil;
import tech.pegasys.teku.validator.api.SubmitDataError;
import tech.pegasys.teku.validator.api.ValidatorApiChannel;
import tech.pegasys.teku.validator.client.loader.OwnedValidators;

@TestSpecContext(milestone = {GLOAS, HEZE})
public class BuilderPreferencesPublisherTest {

  private final ValidatorApiChannel validatorApiChannel = mock(ValidatorApiChannel.class);
  private final BuilderConfigProvider builderConfigProvider = mock(BuilderConfigProvider.class);

  private Spec spec;
  private DataStructureUtil dataStructureUtil;
  private BuilderPreferencesPublisher publisher;
  private Validator validator;
  private BLSPublicKey publicKey;

  private void setUp(final SpecContext specContext) {
    spec = specContext.getSpec();
    dataStructureUtil = specContext.getDataStructureUtil();
    final Signer signer = mock(Signer.class);
    publicKey = dataStructureUtil.randomPublicKey();
    validator = new Validator(publicKey, signer, Optional::empty);
    final OwnedValidators ownedValidators = new OwnedValidators(Map.of(publicKey, validator));
    publisher =
        new BuilderPreferencesPublisher(
            ownedValidators, spec, validatorApiChannel, builderConfigProvider);
    when(validatorApiChannel.sendBuilderPreferences(any()))
        .thenReturn(SafeFuture.completedFuture(List.of()));
  }

  private BuilderConfig builderConfigWithOneEntry() {
    return dataStructureUtil.randomBuilderConfig(1);
  }

  @TestTemplate
  void shouldPublishWhenDutiesIncludeOurValidator(final SpecContext specContext) {
    setUp(specContext);
    final UInt64 epoch = UInt64.valueOf(6);
    final UInt64 slot = spec.computeStartSlotAtEpoch(epoch);
    final BuilderConfig builderConfig = builderConfigWithOneEntry();
    when(builderConfigProvider.getBuilderConfig(validator, slot))
        .thenReturn(SafeFuture.completedFuture(Optional.of(builderConfig)));

    publisher.onProposerDutiesLoaded(
        epoch,
        new ProposerDuties(
            dataStructureUtil.randomBytes32(),
            List.of(new ProposerDuty(publicKey, 42, slot)),
            false));

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<SszList<BuilderPreferencesEntry>> captor =
        ArgumentCaptor.forClass(SszList.class);
    verify(validatorApiChannel).sendBuilderPreferences(captor.capture());
    final SszList<BuilderPreferencesEntry> published = captor.getValue();
    assertThat(published).hasSize(1);
    final BuilderPreferencesEntry preferencesEntry = published.get(0);
    assertThat(preferencesEntry.getProposerPubkey()).isEqualTo(publicKey);
    final BuilderEntry builderEntry = builderConfig.getBuilders().get(0);
    assertThat(preferencesEntry.getUrl()).isEqualTo(builderEntry.getUrl());
    assertThat(preferencesEntry.getAuth()).isEqualTo(builderEntry.getAuth());
    assertThat(preferencesEntry.getMaxExecutionPayment())
        .isEqualTo(builderEntry.getMaxExecutionPayment());
  }

  @TestTemplate
  void shouldNotPublishWhenNoDutiesForOurValidators(final SpecContext specContext) {
    setUp(specContext);
    final UInt64 epoch = UInt64.valueOf(6);
    final UInt64 slot = spec.computeStartSlotAtEpoch(epoch);
    final BLSPublicKey otherKey = dataStructureUtil.randomPublicKey();

    publisher.onProposerDutiesLoaded(
        epoch,
        new ProposerDuties(
            dataStructureUtil.randomBytes32(),
            List.of(new ProposerDuty(otherKey, 99, slot)),
            false));

    verify(validatorApiChannel, never()).sendBuilderPreferences(any());
  }

  @TestTemplate
  void shouldNotPublishWhenBuilderConfigIsAbsent(final SpecContext specContext) {
    setUp(specContext);
    final UInt64 epoch = UInt64.valueOf(6);
    final UInt64 slot = spec.computeStartSlotAtEpoch(epoch);
    when(builderConfigProvider.getBuilderConfig(validator, slot))
        .thenReturn(SafeFuture.completedFuture(Optional.empty()));

    publisher.onProposerDutiesLoaded(
        epoch,
        new ProposerDuties(
            dataStructureUtil.randomBytes32(),
            List.of(new ProposerDuty(publicKey, 42, slot)),
            false));

    verify(validatorApiChannel, never()).sendBuilderPreferences(any());
  }

  @TestTemplate
  void shouldNotPublishWhenBuilderConfigHasNoBuildersConfigured(final SpecContext specContext) {
    setUp(specContext);
    final UInt64 epoch = UInt64.valueOf(6);
    final UInt64 slot = spec.computeStartSlotAtEpoch(epoch);
    when(builderConfigProvider.getBuilderConfig(validator, slot))
        .thenReturn(SafeFuture.completedFuture(Optional.of(BuilderConfig.NO_OP)));

    publisher.onProposerDutiesLoaded(
        epoch,
        new ProposerDuties(
            dataStructureUtil.randomBytes32(),
            List.of(new ProposerDuty(publicKey, 42, slot)),
            false));

    verify(validatorApiChannel, never()).sendBuilderPreferences(any());
  }

  @TestTemplate
  void shouldReceiveValidationRejectionDescription(final SpecContext specContext) {
    setUp(specContext);
    final UInt64 epoch = UInt64.valueOf(6);
    final UInt64 slot = spec.computeStartSlotAtEpoch(epoch);
    final String rejectionDescription = "Invalid builder preferences";
    when(builderConfigProvider.getBuilderConfig(validator, slot))
        .thenReturn(SafeFuture.completedFuture(Optional.of(builderConfigWithOneEntry())));
    when(validatorApiChannel.sendBuilderPreferences(any()))
        .thenReturn(
            SafeFuture.completedFuture(
                List.of(new SubmitDataError(UInt64.ZERO, rejectionDescription))));

    try (LogCaptor logCaptor = LogCaptor.forClass(ValidatorLogger.class)) {
      publisher.onProposerDutiesLoaded(
          epoch,
          new ProposerDuties(
              dataStructureUtil.randomBytes32(),
              List.of(new ProposerDuty(publicKey, 42, slot)),
              false));

      assertThat(logCaptor.getErrorLogs())
          .singleElement()
          .asString()
          .contains("Failed to publish builder preferences for epoch " + epoch);
      assertThat(logCaptor.getErrorThrowables())
          .extracting(error -> error.getCause().getMessage())
          .containsExactly(rejectionDescription);
    }
  }
}
