using System;
using Common.Logging;
using Rhino.ServiceBus.Msmq;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQQueueStrategy
    {
        private readonly RabbitMQConnectionProvider _connectionProvider;
        private readonly Uri _endpoint;
        private readonly ILog _logger = LogManager.GetLogger<RabbitMQQueueStrategy>();

        /// <summary>
        ///     Initializes a new instance of the <see cref="RabbitMQQueueStrategy" /> class.
        /// </summary>
        public RabbitMQQueueStrategy(Uri endpoint, RabbitMQConnectionProvider connectionProvider)
        {
            _endpoint = endpoint;
            _connectionProvider = connectionProvider;
            
            var addr = RabbitMQAddress.From(endpoint);
            SubscriptionQueue = addr.ForSubQueue(SubQueue.Subscriptions).ToUri();
            ErrorQueue = addr.ForSubQueue(SubQueue.Errors).ToUri();
            DiscardedQueue = addr.ForSubQueue(SubQueue.Discarded).ToUri();
        }

        public Uri SubscriptionQueue { get; }
        public Uri ErrorQueue { get; }
        public Uri DiscardedQueue { get; }

        public void InitializeQueue()
        {            
            Initialize(_endpoint);
            Initialize(SubscriptionQueue);
            Initialize(ErrorQueue);
            Initialize(DiscardedQueue);
        }

        private void Initialize(Uri uri)
        {
            var addr = RabbitMQAddress.From(uri);
          
            //_connectionProvider.DeclareExchange(addr, name, "topic");
            _connectionProvider.DeclareQueue(addr, addr.QueueName, true);
            //_connectionProvider.BindQueue(addr, name, name, routingKeys);
        }

        public void PurgeAll()
        {
            Purge(_endpoint);
            Purge(SubscriptionQueue);
            Purge(ErrorQueue);
            Purge(DiscardedQueue);
        }

        private void Purge(Uri uri)
        {
            var addr = RabbitMQAddress.From(uri);
            _connectionProvider.PurgeQueue(addr);
        }
    }
}