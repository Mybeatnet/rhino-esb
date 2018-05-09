using System;
using Common.Logging;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQSubscribeAction : ISubscribeAction
    {
        private readonly RabbitMQConnectionProvider _connectionProvider;

        private readonly ILog _logger = LogManager.GetLogger(typeof(RabbitMQSubscriptionStorage));
        private RabbitMQAddress _busUri;


        public RabbitMQSubscribeAction(RabbitMQConnectionProvider connectionProvider)
        {
            _connectionProvider = connectionProvider;
        }

        public void Unsubscribe(Type type, Endpoint endpoint)
        {
            var broker = RabbitMQAddress.From(endpoint.Uri);
            if (string.IsNullOrEmpty(broker.Exchange))
                throw new SubscriptionException(
                    $"Cannot unsubscribe from endpoint without an exchange for {type}: {endpoint}");
            _connectionProvider.UnbindQueue(broker, broker.Exchange, _busUri.QueueName, type.FullName);
            _logger.InfoFormat("Removed subscription for {0} on {1}", type, endpoint);
        }

        public void Subscribe(Type type, Endpoint endpoint)
        {
            var broker = RabbitMQAddress.From(endpoint.Uri);
            if (string.IsNullOrEmpty(broker.Exchange))
                throw new SubscriptionException(
                    $"Cannot subscribe to endpoint without an exchange for {type}: {endpoint}");

            _connectionProvider.DeclareExchange(broker, broker.Exchange, "direct");
            _connectionProvider.BindQueue(broker, broker.Exchange, _busUri.QueueName, type.FullName);
            _logger.InfoFormat("Added subscription for {0} on {1}", type, endpoint);
        }

        public void Init(IServiceBus bus)
        {
            _busUri = RabbitMQAddress.From(bus.Endpoint.Uri);
        }
    }
}