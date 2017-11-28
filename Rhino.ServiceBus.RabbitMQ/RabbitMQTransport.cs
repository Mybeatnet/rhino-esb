using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Common.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Messages;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQTransport : ITransport
    {
        private static readonly ILog _logger = LogManager.GetLogger<RabbitMQTransport>();

        [ThreadStatic] private static RabbitMQCurrentMessageInformation _currentMessageInformation;        

        private readonly RabbitMQAddress _inputAddress;
        private readonly IMessageBuilder<RabbitMQMessage> _messageBuilder;
        private readonly IMessageSerializer _serializer;
        private readonly ConnectionProvider _connectionProvider;

        public RabbitMQTransport(IMessageSerializer serializer,
            Uri endpoint,
            int threadCount,
            bool consumeInTransaction,
            IMessageBuilder<RabbitMQMessage> messageBuilder)
        {
            _serializer = serializer;
            _messageBuilder = messageBuilder;
            _inputAddress = RabbitMQAddress.From(endpoint);
            Endpoint = new Endpoint {Uri = endpoint, Transactional = consumeInTransaction};
            ThreadCount = threadCount;
            _connectionProvider = new ConnectionProvider();
        }

        public void Start()
        {
            var addr = RabbitMQAddress.From(Endpoint.Uri);

            _consumers = Enumerable.Range(0, ThreadCount)
                .Select(i => new RabbitMQConsumer(i, addr))
                .ToArray();

            foreach (var cons in _consumers)
                cons.Start();
            for (var i = 0; i < ThreadCount; i++)
            {
                _consumers = new RabbitMQConsumer("Rhino Service Bus Worker Thread #" + i, addr)
                threads[i] = new Thread(ReceiveMessage)
                {
                    Name = "Rhino Service Bus Worker Thread #" + i,
                    IsBackground = true
                };
                threads[i].Start(i);
            }


            using (var connection = _connectionProvider.Open(addr, Endpoint.Transactional ?? true))
            {
                connection.BasicQos(0, 100, false);
                var consumer = new EventingBasicConsumer(connection);
                consumer.Received += (o, e) =>
                {                    
                    ProcessMessage(e);
                    connection.BasicAck(e.DeliveryTag,false);                    
                };
                connection.BasicConsume(consumer, addr.QueueName);
            }
        }

        public bool HaveStarted { get; private set; }
        public Endpoint Endpoint { get; }
        public int ThreadCount { get; }
        public CurrentMessageInformation CurrentMessageInformation => _currentMessageInformation;

        public void Send(Endpoint destination, object[] msgs)
        {
            if (HaveStarted == false)
                throw new InvalidOperationException("Cannot send a message before transport is started");

            var messageInformation = new OutgoingMessageInformation
            {
                Destination = destination,
                Messages = msgs,
                Source = Endpoint
            };

            var message = _messageBuilder.BuildFromMessageBatch(messageInformation);

            SendMessageToQueue(message, destination);

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

        private void SendMessageToQueue(RabbitMQMessage message, Endpoint destination)
        {
            var addr = RabbitMQAddress.FromString(destination.Uri.ToString());
            using (var channel = _connectionProvider.Open(addr, true))
            {
                channel.TxSelect();

                var properties = channel.CreateBasicProperties();
                properties.MessageId = message.MessageId.ToString();
                properties.Priority = (byte) message.Priority;
                properties.ReplyTo = message.ReplyTo;
                properties.Expiration = message.Expiration.ToString();
                properties.Headers = message.Headers;
                properties.DeliveryMode = 2; // persistent
                channel.BasicPublish(addr.Exchange, addr.QueueName, true, properties, message.Data);

                channel.TxCommit();
            }
        }

        public void Send(Endpoint endpoint, DateTime processAgainAt, object[] msgs)
        {
            if (HaveStarted == false)
                throw new InvalidOperationException("Cannot send a message before transport is started");

            var messageInformation = new OutgoingMessageInformation
            {
                Destination = endpoint,
                Messages = msgs,
                Source = Endpoint
            };
            var message = _messageBuilder.BuildFromMessageBatch(messageInformation);
            message.Headers["x-delay"] = (int) Math.Max(processAgainAt.Subtract(DateTime.Now).TotalMilliseconds, 0);

            SendMessageToQueue(message, endpoint);
        }

        public void Reply(params object[] messages)
        {
            Send(new Endpoint {Uri = _currentMessageInformation.Source}, messages);
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