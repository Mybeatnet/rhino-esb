using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Common.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQTransport : ITransport
    {
        private static readonly ILog _logger = LogManager.GetLogger<RabbitMQTransport>();

        [ThreadStatic] private static RabbitMQCurrentMessageInformation _currentMessageInformation;

        private readonly RabbitMQConnectionProvider _connectionProvider;
        private readonly RabbitMQAddress _inputAddress;
        private readonly IMessageBuilder<RabbitMQMessage> _messageBuilder;
        private readonly RabbitMQQueueStrategy _queueStrategy;
        private readonly IMessageSerializer _serializer;
        private readonly ITransactionStrategy _txStrategy;

        private RabbitMQConsumer[] _consumers;

        public RabbitMQTransport(IMessageSerializer serializer,
            Uri endpoint,
            int threadCount,
            bool consumeInTransaction,
            int numberOfRetries,
            IMessageBuilder<RabbitMQMessage> messageBuilder,
            ITransactionStrategy txStrategy,
            RabbitMQConnectionProvider connectionProvider,
            RabbitMQQueueStrategy queueStrategy)
        {
            _serializer = serializer;
            _messageBuilder = messageBuilder;
            _txStrategy = txStrategy;
            _connectionProvider = connectionProvider;
            _queueStrategy = queueStrategy;
            _inputAddress = RabbitMQAddress.From(endpoint);
            Endpoint = new Endpoint {Uri = endpoint, Transactional = consumeInTransaction};
            ThreadCount = threadCount;

            new RabbitMQErrorAction(numberOfRetries, this).Init();
            _messageBuilder.Initialize(Endpoint);
        }

        public bool HaveStarted { get; private set; }

        public void Start()
        {
            _consumers = Enumerable.Range(0, ThreadCount)
                .Select(i => new RabbitMQConsumer(i, _connectionProvider, Endpoint, ReceiveMessage))
                .ToArray();

            foreach (var cons in _consumers)
                cons.Start();

            HaveStarted = true;
            Started?.Invoke();
        }

        public Endpoint Endpoint { get; }
        public int ThreadCount { get; }
        public CurrentMessageInformation CurrentMessageInformation => _currentMessageInformation;

        public void Send(Endpoint destination, object[] msgs, RhinoMessagePriority priority)
        {
            if (HaveStarted == false)
                throw new InvalidOperationException("Cannot send a message before transport is started");

            var messageInformation = new OutgoingMessageInformation
            {
                Destination = destination,
                Messages = msgs,
                Source = Endpoint,
                Priority = priority
            };

            var message = _messageBuilder.BuildFromMessageBatch(messageInformation);

            SendMessageToQueue(message, destination, messageInformation);

            var copy = MessageSent;
            if (copy == null)
                return;

            copy(new CurrentMessageInformation
            {
                AllMessages = msgs,
                Source = Endpoint.Uri,
                Destination = destination.Uri,
                MessageId = message.MessageId
            });
        }

        public void Send(Endpoint endpoint, DateTime processAgainAt, object[] msgs, RhinoMessagePriority priority)
        {
            if (HaveStarted == false)
                throw new InvalidOperationException("Cannot send a message before transport is started");

            var messageInformation = new OutgoingMessageInformation
            {
                Destination = endpoint,
                Messages = msgs,
                Source = Endpoint,
                Priority = priority
            };
            var message = _messageBuilder.BuildFromMessageBatch(messageInformation);
            message.Headers["x-delay"] = (int) Math.Max(processAgainAt.Subtract(DateTime.Now).TotalMilliseconds, 0);

            SendMessageToQueue(message, endpoint, messageInformation);
        }

        public void Reply(params object[] messages)
        {
            Send(new Endpoint {Uri = _currentMessageInformation.Source}, messages, RhinoMessagePriority.Normal);
        }

        public event Action<CurrentMessageInformation> MessageSent;
        public event Func<CurrentMessageInformation, bool> AdministrativeMessageArrived;
        public event Func<CurrentMessageInformation, bool> MessageArrived;
        public event Action<CurrentMessageInformation, Exception> MessageSerializationException;
        public event Action<CurrentMessageInformation, Exception> MessageProcessingFailure;
        public event Action<CurrentMessageInformation, Exception> MessageProcessingCompleted;
        public event Action<CurrentMessageInformation> BeforeMessageTransactionRollback;
        public event Action<CurrentMessageInformation> BeforeMessageTransactionCommit;
        public event Action<CurrentMessageInformation, Exception> AdministrativeMessageProcessingCompleted;
        public event Action Started;

        public void Dispose()
        {
            Task.WaitAll(_consumers.Select(x => Task.Run((Action) x.Stop)).ToArray(), 5000);
            foreach (var cons in _consumers)
                cons.Stop();
            _connectionProvider.Dispose();
        }

        private void ReceiveMessage(IModel model, BasicDeliverEventArgs arg)
        {
            var msgType = (MessageType) (int) arg.BasicProperties.Headers["MessageType"];
            switch (msgType)
            {
                case MessageType.AdministrativeMessageMarker:
                    ProcessMessage(model, arg,
                        AdministrativeMessageArrived,
                        AdministrativeMessageProcessingCompleted,
                        null,
                        null);
                    break;
                case MessageType.ShutDownMessageMarker:
                    model.BasicAck(arg.DeliveryTag, false);
                    //ignoring this one
                    break;

                case MessageType.TimeoutMessageMarker:
                default:
                    ProcessMessage(model, arg,
                        MessageArrived,
                        MessageProcessingCompleted,
                        BeforeMessageTransactionCommit,
                        BeforeMessageTransactionRollback);
                    break;
            }
        }

        private void ProcessMessage(IModel model, BasicDeliverEventArgs arg,
            Func<CurrentMessageInformation, bool> messageRecieved,
            Action<CurrentMessageInformation, Exception> messageCompleted,
            Action<CurrentMessageInformation> beforeTransactionCommit,
            Action<CurrentMessageInformation> beforeTransactionRollback)
        {
            using (var tx = _txStrategy.Begin())
            {
                RabbitMQTransaction.Current.Enlist(commit =>
                {
                    if (commit)
                        model.BasicAck(arg.DeliveryTag, false);
                    else
                        model.BasicNack(arg.DeliveryTag, false, true);
                });

                var rabbitMsg = new RabbitMQMessage(arg);

                Exception exception = null;
                var msgInfo = CreateMessageInfo(model, arg, rabbitMsg, null, null);
                try
                {
                    object[] messages = null;
                    using (var ms = new MemoryStream(arg.Body))
                    {
                        messages = _serializer.Deserialize(ms);
                    }

                    try
                    {
                        foreach (var msg in messages)
                        {
                            msgInfo = CreateMessageInfo(model, arg, rabbitMsg, messages, msg);

                            _currentMessageInformation = msgInfo;

                            if (TransportUtil.ProcessSingleMessage(msgInfo, messageRecieved) == false)
                                Discard(msgInfo.Message);
                        }
                    }
                    catch (Exception ex)
                    {
                        exception = ex;
                        _logger.Error("Failed to process message", ex);
                    }
                }
                catch (Exception ex)
                {
                    exception = ex;
                    _logger.Error("Failed to deserialize message", ex);
                    MessageSerializationException?.Invoke(msgInfo, ex);
                }
                finally
                {
                    Action sendMessageBackToQueue = null;
                    if (Endpoint.Transactional == false)
                        sendMessageBackToQueue = () => SendMessageToQueue(rabbitMsg, Endpoint, null);
                    var messageHandlingCompletion = new MessageHandlingCompletion(tx, sendMessageBackToQueue, exception,
                        messageCompleted, beforeTransactionCommit, beforeTransactionRollback, _logger,
                        MessageProcessingFailure, msgInfo);
                    messageHandlingCompletion.HandleMessageCompletion();
                    _currentMessageInformation = null;
                }
            }
        }

        private RabbitMQCurrentMessageInformation CreateMessageInfo(IModel model, BasicDeliverEventArgs arg,
            RabbitMQMessage rabbitMsg, object[] messages, object msg)
        {
            var msgInfo = new RabbitMQCurrentMessageInformation(rabbitMsg)
            {
                TransportMessageId = arg.BasicProperties.MessageId,
                Destination = Endpoint.Uri,
                Source = new Uri(arg.BasicProperties.ReplyTo),
                Model = model,
                ListenUri = Endpoint.Uri,
                AllMessages = messages,
                Message = msg
            };
            return msgInfo;
        }

        private void SendMessageToQueue(RabbitMQMessage message, Endpoint destination,
            OutgoingMessageInformation messageInformation)
        {
            var addr = RabbitMQAddress.From(destination.Uri);
            using (var channel = _connectionProvider.Open(addr, true))
            {
                var properties = channel.CreateBasicProperties();
                message.Populate(properties);

                if (!string.IsNullOrEmpty(addr.QueueName))
                {
                    channel.BasicPublish("", addr.QueueName, true, properties, message.Data);
                }
                else
                {
                    var routingKey = addr.RoutingKeys;
                    if (string.IsNullOrEmpty(routingKey) && messageInformation != null)
                        routingKey = messageInformation.Messages[0].GetType().FullName;

                    channel.BasicPublish(addr.Exchange, routingKey, true, properties, message.Data);
                }
            }
        }

        private void Discard(object message)
        {
            _logger.DebugFormat("Discarding message {0} ({1}) because there are no consumers for it.",
                message, _currentMessageInformation.TransportMessageId);
            Send(new Endpoint {Uri = _queueStrategy.DiscardedQueue}, new[] {message}, RhinoMessagePriority.Low);
        }


        public IEnumerable<RabbitMQMessage> ReadMessages(string subqueue = null)
        {
            var addr = RabbitMQAddress.From(Endpoint.Uri);
            if (subqueue != null)
                addr.QueueName += "." + subqueue;
            using (var channel = _connectionProvider.Open(addr, true))
            {
                while (true)
                {
                    var msg = channel.BasicGet(addr.QueueName, false);
                    if (msg == null)
                        yield break;

                    RabbitMQTransaction.Current.Enlist(commit =>
                    {
                        if (commit)
                            channel.BasicAck(msg.DeliveryTag, false);
                        else
                            channel.BasicNack(msg.DeliveryTag, false, true);
                    });
                    yield return new RabbitMQMessage(msg.Body, msg.BasicProperties);
                }
            }
        }

        public void SendToErrorQueue(RabbitMQMessage msg)
        {
            SendMessageToQueue(msg,
                new Endpoint {Uri = _queueStrategy.ErrorQueue, Transactional = Endpoint.Transactional}, null);
        }
    }
}