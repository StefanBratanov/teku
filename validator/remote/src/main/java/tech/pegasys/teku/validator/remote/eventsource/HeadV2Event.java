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

package tech.pegasys.teku.validator.remote.eventsource;

import static tech.pegasys.teku.ethereum.json.types.EthereumTypes.MILESTONE_TYPE;
import static tech.pegasys.teku.infrastructure.json.types.CoreTypes.BOOLEAN_TYPE;
import static tech.pegasys.teku.infrastructure.json.types.CoreTypes.BYTES32_TYPE;
import static tech.pegasys.teku.infrastructure.json.types.CoreTypes.UINT64_TYPE;
import static tech.pegasys.teku.spec.datastructures.forkchoice.ForkChoicePayloadStatus.PAYLOAD_STATUS_EMPTY;
import static tech.pegasys.teku.spec.datastructures.forkchoice.ForkChoicePayloadStatus.PAYLOAD_STATUS_FULL;
import static tech.pegasys.teku.spec.datastructures.forkchoice.ForkChoicePayloadStatus.PAYLOAD_STATUS_PENDING;

import org.apache.tuweni.bytes.Bytes32;
import tech.pegasys.teku.infrastructure.json.types.DeserializableTypeDefinition;
import tech.pegasys.teku.infrastructure.unsigned.UInt64;
import tech.pegasys.teku.spec.SpecMilestone;
import tech.pegasys.teku.spec.datastructures.forkchoice.ForkChoicePayloadStatus;

record HeadV2Event(SpecMilestone version, Data data) {

  record Data(
      UInt64 slot,
      Bytes32 block,
      Bytes32 state,
      ForkChoicePayloadStatus payloadStatus,
      boolean epochTransition,
      Bytes32 currentEpochDependentRoot,
      Bytes32 nextEpochDependentRoot,
      boolean executionOptimistic) {}

  private static final DeserializableTypeDefinition<ForkChoicePayloadStatus> PAYLOAD_STATUS_TYPE =
      DeserializableTypeDefinition.string(ForkChoicePayloadStatus.class)
          .formatter(
              payloadStatus ->
                  switch (payloadStatus) {
                    case PAYLOAD_STATUS_EMPTY -> "empty";
                    case PAYLOAD_STATUS_FULL -> "full";
                    case PAYLOAD_STATUS_PENDING -> "pending";
                  })
          .parser(
              payloadStatus ->
                  switch (payloadStatus) {
                    case "empty" -> PAYLOAD_STATUS_EMPTY;
                    case "full" -> PAYLOAD_STATUS_FULL;
                    case "pending" -> PAYLOAD_STATUS_PENDING;
                    default ->
                        throw new IllegalArgumentException(
                            "Unknown payload status: " + payloadStatus);
                  })
          .format("string")
          .build();

  private static final DeserializableTypeDefinition<Data> DATA_TYPE_DEFINITION =
      DeserializableTypeDefinition.object(Data.class, DataBuilder.class)
          .initializer(DataBuilder::new)
          .finisher(DataBuilder::build)
          .withField("slot", UINT64_TYPE, Data::slot, DataBuilder::slot)
          .withField("block", BYTES32_TYPE, Data::block, DataBuilder::block)
          .withField("state", BYTES32_TYPE, Data::state, DataBuilder::state)
          .withField(
              "payload_status",
              PAYLOAD_STATUS_TYPE,
              Data::payloadStatus,
              DataBuilder::payloadStatus)
          .withField(
              "epoch_transition", BOOLEAN_TYPE, Data::epochTransition, DataBuilder::epochTransition)
          .withField(
              "current_epoch_dependent_root",
              BYTES32_TYPE,
              Data::currentEpochDependentRoot,
              DataBuilder::currentEpochDependentRoot)
          .withField(
              "next_epoch_dependent_root",
              BYTES32_TYPE,
              Data::nextEpochDependentRoot,
              DataBuilder::nextEpochDependentRoot)
          .withField(
              "execution_optimistic",
              BOOLEAN_TYPE,
              Data::executionOptimistic,
              DataBuilder::executionOptimistic)
          .build();

  static final DeserializableTypeDefinition<HeadV2Event> TYPE_DEFINITION =
      DeserializableTypeDefinition.object(HeadV2Event.class, HeadV2EventBuilder.class)
          .initializer(HeadV2EventBuilder::new)
          .finisher(HeadV2EventBuilder::build)
          .withField("version", MILESTONE_TYPE, HeadV2Event::version, HeadV2EventBuilder::version)
          .withField("data", DATA_TYPE_DEFINITION, HeadV2Event::data, HeadV2EventBuilder::data)
          .build();

  private static class DataBuilder {
    private UInt64 slot;
    private Bytes32 block;
    private Bytes32 state;
    private ForkChoicePayloadStatus payloadStatus;
    private boolean epochTransition;
    private Bytes32 currentEpochDependentRoot;
    private Bytes32 nextEpochDependentRoot;
    private boolean executionOptimistic;

    DataBuilder slot(final UInt64 slot) {
      this.slot = slot;
      return this;
    }

    DataBuilder block(final Bytes32 block) {
      this.block = block;
      return this;
    }

    DataBuilder state(final Bytes32 state) {
      this.state = state;
      return this;
    }

    DataBuilder payloadStatus(final ForkChoicePayloadStatus payloadStatus) {
      this.payloadStatus = payloadStatus;
      return this;
    }

    DataBuilder epochTransition(final boolean epochTransition) {
      this.epochTransition = epochTransition;
      return this;
    }

    DataBuilder currentEpochDependentRoot(final Bytes32 currentEpochDependentRoot) {
      this.currentEpochDependentRoot = currentEpochDependentRoot;
      return this;
    }

    DataBuilder nextEpochDependentRoot(final Bytes32 nextEpochDependentRoot) {
      this.nextEpochDependentRoot = nextEpochDependentRoot;
      return this;
    }

    DataBuilder executionOptimistic(final boolean executionOptimistic) {
      this.executionOptimistic = executionOptimistic;
      return this;
    }

    Data build() {
      return new Data(
          slot,
          block,
          state,
          payloadStatus,
          epochTransition,
          currentEpochDependentRoot,
          nextEpochDependentRoot,
          executionOptimistic);
    }
  }

  private static class HeadV2EventBuilder {
    private SpecMilestone version;
    private Data data;

    HeadV2EventBuilder version(final SpecMilestone version) {
      this.version = version;
      return this;
    }

    HeadV2EventBuilder data(final Data data) {
      this.data = data;
      return this;
    }

    HeadV2Event build() {
      return new HeadV2Event(version, data);
    }
  }
}
