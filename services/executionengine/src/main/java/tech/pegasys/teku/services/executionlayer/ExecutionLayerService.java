/*
 * Copyright 2021 ConsenSys AG.
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

package tech.pegasys.teku.services.executionlayer;

import static com.google.common.base.Preconditions.checkState;
import static tech.pegasys.teku.infrastructure.logging.EventLogger.EVENT_LOG;
import static tech.pegasys.teku.spec.config.Constants.EXECUTION_TIMEOUT;

import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import tech.pegasys.teku.ethereum.executionengine.ExecutionClientProvider;
import tech.pegasys.teku.ethereum.executionlayer.ExecutionLayerChannelImpl;
import tech.pegasys.teku.infrastructure.async.AsyncRunner;
import tech.pegasys.teku.infrastructure.async.SafeFuture;
import tech.pegasys.teku.infrastructure.async.timed.RepeatingTaskScheduler;
import tech.pegasys.teku.infrastructure.events.EventChannels;
import tech.pegasys.teku.infrastructure.time.TimeProvider;
import tech.pegasys.teku.infrastructure.unsigned.UInt64;
import tech.pegasys.teku.service.serviceutils.Service;
import tech.pegasys.teku.service.serviceutils.ServiceConfig;
import tech.pegasys.teku.spec.executionlayer.ExecutionLayerChannel;
import tech.pegasys.teku.spec.executionlayer.ExecutionLayerChannelStub;

public class ExecutionLayerService extends Service {

  private static final Logger LOG = LogManager.getLogger();

  private final EventChannels eventChannels;
  private final ExecutionClientProvider engineWeb3jClientProvider;
  private final Optional<ExecutionClientProvider> builderWeb3jClientProvider;
  private final TimeProvider timeProvider;
  private final ExecutionLayerChannel executionLayerChannel;
  private final AsyncRunner asyncRunner;

  public ExecutionLayerService(
      final ServiceConfig serviceConfig, final ExecutionLayerConfiguration config) {
    this.eventChannels = serviceConfig.getEventChannels();
    this.engineWeb3jClientProvider =
        ExecutionClientProvider.create(
            config.getEngineEndpoint(),
            serviceConfig.getTimeProvider(),
            EXECUTION_TIMEOUT,
            config.getEngineJwtSecretFile(),
            serviceConfig.getDataDirLayout().getBeaconDataDirectory());

    this.builderWeb3jClientProvider =
        config
            .getBuilderEndpoint()
            .map(
                builderEndpoint ->
                    ExecutionClientProvider.create(
                        builderEndpoint,
                        serviceConfig.getTimeProvider(),
                        EXECUTION_TIMEOUT,
                        Optional.empty(),
                        serviceConfig.getDataDirLayout().getBeaconDataDirectory()));

    final boolean builderIsStub =
        builderWeb3jClientProvider.map(ExecutionClientProvider::isStub).orElse(false);

    checkState(
        engineWeb3jClientProvider.isStub() == builderIsStub,
        "mixed configuration with stubbed and non-stubbed execution layer endpoints is not supported");

    this.timeProvider = serviceConfig.getTimeProvider();
    this.asyncRunner = serviceConfig.createAsyncRunner("execution-layer");

    final String endpoint = engineWeb3jClientProvider.getEndpoint();
    LOG.info("Using execution engine at {}", endpoint);
    if (engineWeb3jClientProvider.isStub()) {
      EVENT_LOG.executionLayerStubEnabled();
      this.executionLayerChannel =
          new ExecutionLayerChannelStub(config.getSpec(), timeProvider, true);
    } else {
      final MetricsSystem metricsSystem = serviceConfig.getMetricsSystem();
      this.executionLayerChannel =
          ExecutionLayerChannelImpl.create(
              engineWeb3jClientProvider.getWeb3JClient(),
              builderWeb3jClientProvider.map(ExecutionClientProvider::getWeb3JClient),
              config.getEngineVersion(),
              config.getSpec(),
              metricsSystem);
    }
  }

  @Override
  protected SafeFuture<?> doStart() {
    eventChannels.subscribe(ExecutionLayerChannel.class, executionLayerChannel);
    final RepeatingTaskScheduler taskScheduler =
        new RepeatingTaskScheduler(asyncRunner, timeProvider);
    taskScheduler.scheduleRepeatingEvent(
        timeProvider.getTimeInSeconds(),
        UInt64.valueOf(30),
        (scheduledTime, actualTime) -> performBuilderHealthCheck());
    return SafeFuture.COMPLETE;
  }

  @Override
  protected SafeFuture<?> doStop() {
    asyncRunner.shutdown();
    return SafeFuture.COMPLETE;
  }

  public Optional<ExecutionClientProvider> getEngineWeb3jClientProvider() {
    return engineWeb3jClientProvider.isStub()
        ? Optional.empty()
        : Optional.of(engineWeb3jClientProvider);
  }

  private void performBuilderHealthCheck() {
    if (builderWeb3jClientProvider.isEmpty()) {
      return;
    }
    executionLayerChannel
        .builderStatus()
        .finish(
            status -> {
              if (status.hasFailed()) {
                LOG.error(
                    "The execution builder has failed health check: {}", status.getErrorMessage());
              }
            },
            throwable ->
                LOG.error("Error while performing the execution builder health check", throwable));
  }
}
