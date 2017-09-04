using System;
using System.IO;
using Common.Logging;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQTransport : ITransport
    {
        [ThreadStatic] private static RabbitMQCurrentMessageInformation _currentMessageInformation;
        private readonly IEndpointRouter _endpointRouter;

        private readonly RabbitMQAddress _inputAddress;
        private readonly ILog _logger = LogManager.GetLogger<RabbitMQTransport>();
        private readonly IMessageBuilder<RabbitMQMessage> _messageBuilder;
        private readonly IMessageSerializer _serializer;
        private readonly ConnectionProvider connectionProvider;

        public RabbitMQTransport(IMessageSerializer serializer,
            Uri endpoint,
            int threadCount,
            IEndpointRouter endpointRouter,
            bool consumeInTransaction,
            IMessageBuilder<RabbitMQMessage> messageBuilder)
        {
            _serializer = serializer;
            _endpointRouter = endpointRouter;
            _messageBuilder = messageBuilder;
            _inputAddress = RabbitMQAddress.From(endpoint);
            Endpoint = new Endpoint {Uri = endpoint, Transactional = consumeInTransaction};
            ThreadCount = threadCount;
            connectionProvider = new ConnectionProvider();
        }

        public void Start()
        {
            throw new NotImplementedException();
        }

        public Endpoint Endpoint { get; }
        public int ThreadCount { get; }
        public CurrentMessageInformation CurrentMessageInformation => _currentMessageInformation;

        public void Send(Endpoint destination, object[] msgs)
        {
            var address = RabbitMQAddress.From(destination.Uri);

            //if sending locally we need to munge some stuff (remove routing keys)
            if (address == _inputAddress)
                address =
                    new RabbitMQAddress(
                        _inputAddress.Broker,
                        _inputAddress.VirtualHost,
                        _inputAddress.Username,
                        _inputAddress.Password,
                        _inputAddress.Exchange,
                        _inputAddress.QueueName,
                        string.Empty,
                        false);

            var outgoingMessage = new OutgoingMessageInformation
            {
                Destination = destination,
                Messages = msgs,
                Source = Endpoint
            };

            var transportMessage = _messageBuilder.BuildFromMessageBatch(outgoingMessage);

            using (var channel = connectionProvider.Open(address, true))
            {
                var messageId = Guid.NewGuid().ToString();
                var properties = channel.CreateBasicProperties();
                properties.MessageId = messageId;
                if (!string.IsNullOrEmpty(transportMessage.Properties.CorrelationId))
                    properties.CorrelationId = transportMessage.CorrelationId;

                properties.Timestamp = DateTime.UtcNow.ToAmqpTimestamp();
                properties.ReplyTo = this.InputAddress;
                properties.SetPersistent(transportMessage.Recoverable);
                var headers = transportMessage.Headers;
                if ((headers != null) && (headers.Count > 0))
                {
                    var dictionary = headers
                        .ToDictionary<HeaderInfo, string, object>
                        (entry => entry.Key, entry => entry.Value);

                    properties.Headers = dictionary;
                }

                if (address.RouteByType)
                {
                    var type = msgs[0].GetType();
                    string typeName = type.FullName;
                    _logger.InfoFormat("Sending message (routed) " + address.ToString(typeName) + " of " + type.Name);
                    channel.BasicPublish(address.Exchange, typeName, true, properties, transportMessage.Data);
                    return;
                }

                var routingKeys = address.GetRoutingKeysAsArray();

                if (routingKeys.Length > 1)
                {
                    var message = "Too many Routing Keys specified for endpoint: " + address;
                    message += Environment.NewLine + "Keys: " + address.RoutingKeys;
                    _logger.Error(message);
                    throw new InvalidOperationException(message);
                }

                if (routingKeys.Length > 0)
                {
                    var type = msgs[0].GetType();
                    _logger.InfoFormat("Sending message (routed) " + address + " of " + type.Name);
                    channel.BasicPublish(address.Exchange, routingKeys[0], true, properties, transportMessage.Data);
                    return;
                }

                _logger.Info("Sending message " + address + " of " + transportMessage.Body[0].GetType().Name);
                channel.BasicPublish(address.Exchange, address.QueueName, properties, stream.ToArray());
                transportMessage.Id = properties.MessageId;
            }
        }

        public void Send(Endpoint endpoint, DateTime processAgainAt, object[] msgs)
        {
            throw new NotImplementedException();
        }

        public void Reply(params object[] messages)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }
    }
}