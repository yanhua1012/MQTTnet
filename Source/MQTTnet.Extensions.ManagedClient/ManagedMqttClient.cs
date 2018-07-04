using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet.Client;
using MQTTnet.Diagnostics;
using MQTTnet.Exceptions;
using MQTTnet.Protocol;

namespace MQTTnet.Extensions.ManagedClient
{
    public class ManagedSubscription
    {
        public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

        public bool IsPublished { get; set; }
    }

    public class ManagedMqttClient : IManagedMqttClient
    {
        private readonly BlockingCollection<ManagedMqttApplicationMessage> _messageQueue = new BlockingCollection<ManagedMqttApplicationMessage>();

        //private readonly Dictionary<string, MqttQualityOfServiceLevel> _subscriptions = new Dictionary<string, MqttQualityOfServiceLevel>();

        private readonly Dictionary<string, ManagedSubscription> _subscriptions = new Dictionary<string, ManagedSubscription>();
        private readonly HashSet<string> _unsubscriptions = new HashSet<string>();

        private readonly IMqttClient _mqttClient;
        private readonly IMqttNetChildLogger _logger;

        private CancellationTokenSource _connectionCancellationToken;
        private CancellationTokenSource _publishingCancellationToken;

        private ManagedMqttClientStorageManager _storageManager;
        private IManagedMqttClientOptions _options;

        private bool _subscriptionsNotPushed;

        public ManagedMqttClient(IMqttClient mqttClient, IMqttNetChildLogger logger)
        {
            if (logger == null) throw new ArgumentNullException(nameof(logger));

            _mqttClient = mqttClient ?? throw new ArgumentNullException(nameof(mqttClient));

            _mqttClient.Connected += OnConnected;
            _mqttClient.Disconnected += OnDisconnected;
            _mqttClient.ApplicationMessageReceived += OnApplicationMessageReceived;

            _logger = logger.CreateChildLogger(nameof(ManagedMqttClient));
        }

        public bool IsConnected => _mqttClient.IsConnected;
        public bool IsStarted => _connectionCancellationToken != null;
        public int PendingApplicationMessagesCount => _messageQueue.Count;

        public event EventHandler<MqttClientConnectedEventArgs> Connected;
        public event EventHandler<MqttClientDisconnectedEventArgs> Disconnected;

        public event EventHandler<MqttApplicationMessageReceivedEventArgs> ApplicationMessageReceived;
        public event EventHandler<ApplicationMessageProcessedEventArgs> ApplicationMessageProcessed;

        public event EventHandler<MqttManagedProcessFailedEventArgs> ConnectingFailed;
        public event EventHandler<MqttManagedProcessFailedEventArgs> SynchronizingSubscriptionsFailed;

        public async Task StartAsync(IManagedMqttClientOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options.ClientOptions == null) throw new ArgumentException("The client options are not set.", nameof(options));

            if (!options.ClientOptions.CleanSession)
            {
                throw new NotSupportedException("The managed client does not support existing sessions.");
            }

            if (_connectionCancellationToken != null) throw new InvalidOperationException("The managed client is already started.");

            _options = options;

            if (_options.Storage != null)
            {
                _storageManager = new ManagedMqttClientStorageManager(_options.Storage);
                var messages = await _storageManager.LoadQueuedMessagesAsync().ConfigureAwait(false);

                foreach (var message in messages)
                {
                    _messageQueue.Add(message);
                }
            }

            _connectionCancellationToken = new CancellationTokenSource();

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Task.Run(() => MaintainConnectionAsync(_connectionCancellationToken.Token), _connectionCancellationToken.Token);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            _logger.Info("Started");
        }

        public Task StopAsync()
        {
            StopPublishing();
            StopMaintainingConnection();

            while (_messageQueue.Any())
            {
                _messageQueue.Take();
            }

            return Task.FromResult(0);
        }

        public Task PublishAsync(MqttApplicationMessage applicationMessage)
        {
            if (applicationMessage == null) throw new ArgumentNullException(nameof(applicationMessage));

            return PublishAsync(new ManagedMqttApplicationMessageBuilder().WithApplicationMessage(applicationMessage).Build());
        }

        public async Task PublishAsync(ManagedMqttApplicationMessage applicationMessage)
        {
            if (applicationMessage == null) throw new ArgumentNullException(nameof(applicationMessage));

            if (_storageManager != null)
            {
                await _storageManager.AddAsync(applicationMessage).ConfigureAwait(false);
            }

            _messageQueue.Add(applicationMessage);
        }

        public Task SubscribeAsync(IEnumerable<TopicFilter> topicFilters)
        {
            if (topicFilters == null) throw new ArgumentNullException(nameof(topicFilters));

            lock (_subscriptions)
            {
                foreach (var topicFilter in topicFilters)
                {
                    if (_subscriptions.TryGetValue(topicFilter.Topic, out var existingSubscription))
                    {
                        if (existingSubscription.QualityOfServiceLevel == topicFilter.QualityOfServiceLevel &&
                            existingSubscription.IsPublished)
                        {
                            _logger.Info($"Skipping subscription if topic '{topicFilter}' because it is already subscribed.");
                            continue;
                        }
                    }

                    _subscriptions[topicFilter.Topic] = new ManagedSubscription
                    {
                        QualityOfServiceLevel = topicFilter.QualityOfServiceLevel,
                        IsPublished = false
                    };
                }
            }

            Task.Run(TrySynchronizeSubscriptionsAsync);

            ////lock (_subscriptions)
            ////{
            ////    foreach (var topicFilter in topicFilters)
            ////    {
            ////        _subscriptions[topicFilter.Topic] = topicFilter.QualityOfServiceLevel;
            ////        _subscriptionsNotPushed = true;
            ////    }
            ////}

            return Task.FromResult(0);
        }

        public Task UnsubscribeAsync(IEnumerable<string> topics)
        {
            if (topics == null) throw new ArgumentNullException(nameof(topics));

            lock (_subscriptions)
            {
                foreach (var topic in topics)
                {
                    if (_subscriptions.Remove(topic))
                    {
                        _unsubscriptions.Add(topic);
                        ////_subscriptionsNotPushed = true;
                    }
                }
            }

            Task.Run(TrySynchronizeSubscriptionsAsync);

            return Task.FromResult(0);
        }

        public void Dispose()
        {
            _messageQueue?.Dispose();
            _connectionCancellationToken?.Dispose();
            _publishingCancellationToken?.Dispose();
        }

        private async Task MaintainConnectionAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await TryMaintainConnectionAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                _logger.Error(exception, "Unhandled exception while maintaining connection.");
            }
            finally
            {
                await _mqttClient.DisconnectAsync().ConfigureAwait(false);
                _logger.Info("Stopped");
            }
        }

        private async Task TryMaintainConnectionAsync(CancellationToken cancellationToken)
        {
            try
            {
                var ensureIsConnectedResult = await EnsureIsConnectedAsync().ConfigureAwait(false);

                if (ensureIsConnectedResult.ConnectionStatus == ConnectionStatus.StillConnected)
                {
                    await Task.Delay(_options.ConnectionCheckInterval, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (ensureIsConnectedResult.ConnectionStatus == ConnectionStatus.NotConnected)
                {
                    StopPublishing();

                    await Task.Delay(_options.AutoReconnectDelay, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (ensureIsConnectedResult.ConnectionStatus == ConnectionStatus.Reconnected || _subscriptionsNotPushed)
                {
                    // Reset the subscription status if the session is new to force
                    // a resend of all subscriptions.
                    if (!ensureIsConnectedResult.IsExistingSession)
                    {
                        lock (_subscriptions)
                        {
                            foreach (var subscription in _subscriptions)
                            {
                                subscription.Value.IsPublished = false;
                            }
                        }
                    }

                    await TrySynchronizeSubscriptionsAsync().ConfigureAwait(false);
                    StartPublishing();
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (MqttCommunicationException exception)
            {
                _logger.Warning(exception, "Communication exception while maintaining connection.");
            }
            catch (Exception exception)
            {
                _logger.Error(exception, "Unhandled exception while maintaining connection.");
            }
        }

        private void TryPublishQueuedMessages(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var message = _messageQueue.Take(cancellationToken);
                    if (message == null)
                    {
                        continue;
                    }

                    cancellationToken.ThrowIfCancellationRequested();

                    TryPublishQueuedMessage(message);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                _logger.Error(exception, "Unhandled exception while publishing queued application messages.");
            }
            finally
            {
                _logger.Verbose("Stopped publishing messages.");
            }
        }

        private void TryPublishQueuedMessage(ManagedMqttApplicationMessage message)
        {
            Exception transmitException = null;
            try
            {
                _mqttClient.PublishAsync(message.ApplicationMessage).GetAwaiter().GetResult();
                _storageManager?.RemoveAsync(message).GetAwaiter().GetResult();
            }
            catch (MqttCommunicationException exception)
            {
                transmitException = exception;

                _logger.Warning(exception, $"Publishing application ({message.Id}) message failed.");

                if (message.ApplicationMessage.QualityOfServiceLevel > MqttQualityOfServiceLevel.AtMostOnce)
                {
                    _messageQueue.Add(message);
                }
            }
            catch (Exception exception)
            {
                transmitException = exception;
                _logger.Error(exception, $"Unhandled exception while publishing application message ({message.Id}).");
            }
            finally
            {
                ApplicationMessageProcessed?.Invoke(this, new ApplicationMessageProcessedEventArgs(message, transmitException));
            }
        }

        private Task TrySynchronizeSubscriptionsAsync()
        {
            _logger.Info(nameof(ManagedMqttClient), "Synchronizing subscriptions.");

            // TODO: Make async.

            try
            {
                lock (_subscriptions)
                {
                    if (!_subscriptions.Any(s => s.Value.IsPublished))
                    {
                        _logger.Verbose("No pending subscriptions.");
                    }
                    else
                    {
                        foreach (var subscription in _subscriptions)
                        {
                            if (subscription.Value.IsPublished)
                            {
                                continue;
                            }

                            var result = _mqttClient.SubscribeAsync(subscription.Key, subscription.Value.QualityOfServiceLevel).GetAwaiter().GetResult();
                            if (result[0].ReturnCode == MqttSubscribeReturnCode.Failure)
                            {
                                _logger.Warning(null, $"Server returned _failure_ for subscription '{subscription.Key}@{subscription.Value.QualityOfServiceLevel}'.");
                            }
                            else
                            {
                                _logger.Verbose($"Published subscription '{subscription.Key}@{subscription.Value.QualityOfServiceLevel}'.");
                                subscription.Value.IsPublished = true;
                            }
                        }
                    }
                }

                lock (_unsubscriptions)
                {
                    if (!_subscriptions.Any(s => s.Value.IsPublished))
                    {
                        _logger.Verbose("No pending unsubscriptions.");
                    }
                    else
                    {
                        foreach (var unsubscription in _unsubscriptions.ToList())
                        {
                            _mqttClient.UnsubscribeAsync(unsubscription).GetAwaiter().GetResult();

                            _unsubscriptions.Remove(unsubscription);
                        }
                    }
                }
            }
            catch (Exception exception)
            {
                _logger.Warning(exception, "Synchronizing subscriptions failed.");
                SynchronizingSubscriptionsFailed?.Invoke(this, new MqttManagedProcessFailedEventArgs(exception));
            }

            return Task.FromResult(0);




            ////_logger.Info(nameof(ManagedMqttClient), "Synchronizing subscriptions");

            ////List<TopicFilter> subscriptions;
            ////HashSet<string> unsubscriptions;

            ////lock (_subscriptions)
            ////{
            ////    subscriptions = _subscriptions.Select(i => new TopicFilter(i.Key, i.Value)).ToList();

            ////    unsubscriptions = new HashSet<string>(_unsubscriptions);
            ////    _unsubscriptions.Clear();

            ////    _subscriptionsNotPushed = false;
            ////}

            ////if (!subscriptions.Any() && !unsubscriptions.Any())
            ////{
            ////    return;
            ////}

            ////try
            ////{
            ////    if (subscriptions.Any())
            ////    {
            ////        await _mqttClient.SubscribeAsync(subscriptions).ConfigureAwait(false);
            ////    }

            ////    if (unsubscriptions.Any())
            ////    {
            ////        await _mqttClient.UnsubscribeAsync(unsubscriptions).ConfigureAwait(false);
            ////    }
            ////}
            ////catch (Exception exception)
            ////{
            ////    _logger.Warning(exception, "Synchronizing subscriptions failed.");
            ////    _subscriptionsNotPushed = true;

            ////    SynchronizingSubscriptionsFailed?.Invoke(this, new MqttManagedProcessFailedEventArgs(exception));
            ////}
        }

        public class EnsureIsConnectedResult
        {
            public ConnectionStatus ConnectionStatus { get; set; }

            public bool IsExistingSession { get; set; }
        }

        private async Task<EnsureIsConnectedResult> EnsureIsConnectedAsync()
        {
            if (_mqttClient.IsConnected)
            {
                return new EnsureIsConnectedResult
                {
                    ConnectionStatus = ConnectionStatus.StillConnected,
                    IsExistingSession = true
                };
            }

            try
            {
                var connectResult = await _mqttClient.ConnectAsync(_options.ClientOptions).ConfigureAwait(false);

                return new EnsureIsConnectedResult
                {
                    ConnectionStatus = ConnectionStatus.Reconnected,
                    IsExistingSession = connectResult.IsSessionPresent
                };
            }
            catch (Exception exception)
            {
                ConnectingFailed?.Invoke(this, new MqttManagedProcessFailedEventArgs(exception));

                return new EnsureIsConnectedResult
                {
                    ConnectionStatus = ConnectionStatus.NotConnected,
                    IsExistingSession = false
                };
            }
        }

        private void OnApplicationMessageReceived(object sender, MqttApplicationMessageReceivedEventArgs eventArgs)
        {
            ApplicationMessageReceived?.Invoke(this, eventArgs);
        }

        private void OnDisconnected(object sender, MqttClientDisconnectedEventArgs eventArgs)
        {
            Disconnected?.Invoke(this, eventArgs);
        }

        private void OnConnected(object sender, MqttClientConnectedEventArgs eventArgs)
        {
            Connected?.Invoke(this, eventArgs);
        }

        private void StartPublishing()
        {
            if (_publishingCancellationToken != null)
            {
                StopPublishing();
            }

            var cts = new CancellationTokenSource();

            _publishingCancellationToken = cts;

            // The message publishing thread uses no async/await to increase performance.
            // Blocking one single thread for this is an acceptable strategy for a client
            // library.
            Task.Factory.StartNew(() => TryPublishQueuedMessages(cts.Token), cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private void StopPublishing()
        {
            _publishingCancellationToken?.Cancel(false);
            _publishingCancellationToken?.Dispose();
            _publishingCancellationToken = null;
        }

        private void StopMaintainingConnection()
        {
            _connectionCancellationToken?.Cancel(false);
            _connectionCancellationToken?.Dispose();
            _connectionCancellationToken = null;
        }
    }
}
