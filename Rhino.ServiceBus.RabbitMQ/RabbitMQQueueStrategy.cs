using System;
using System.Collections.Generic;
using Common.Logging;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
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
            ErrorQueue = addr.ForSubQueue(SubQueue.Errors).ToUri();
            DiscardedQueue = addr.ForSubQueue(SubQueue.Discarded).ToUri();

            DelayedExchange = "delayed-exchange";
        }

        public Uri ErrorQueue { get; }
        public Uri DiscardedQueue { get; }
        public string DelayedExchange { get; }

        public void InitializeQueue()
        {
            Initialize(_endpoint);
            InitializeDelayedExchange();            
            Initialize(ErrorQueue);
            Initialize(DiscardedQueue);
        }

        private void InitializeDelayedExchange()
        {
            var addr = RabbitMQAddress.From(_endpoint);
            var args = new Dictionary<string, object>();
            args["x-delayed-type"] = "direct";
            _connectionProvider.DeclareExchange(addr, DelayedExchange, "x-delayed-message", true, false, args);
            _connectionProvider.BindQueue(addr, DelayedExchange, addr.QueueName, addr.QueueName);
        }

        private void Initialize(Uri uri)
        {
            var addr = RabbitMQAddress.From(uri);
            
            _connectionProvider.DeclareQueue(addr, addr.QueueName, true);
        }

        public void PurgeAll()
        {
            Purge(_endpoint);            
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