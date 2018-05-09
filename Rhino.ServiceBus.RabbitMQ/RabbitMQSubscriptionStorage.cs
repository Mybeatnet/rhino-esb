using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQSubscriptionStorage : ISubscriptionStorage
    {
        private readonly ILog _logger = LogManager.GetLogger(typeof(RabbitMQSubscriptionStorage));
        private readonly IEnumerable<MessageOwner> _messageOwners;


        public RabbitMQSubscriptionStorage(IEnumerable<MessageOwner> messageOwners)
        {
            _messageOwners = messageOwners;
        }

        public void Initialize()
        {
        }

        public IEnumerable<Uri> GetSubscriptionsFor(Type type)
        {
            // we have to return the exchange uri to send the message to - this is just the message owner
            var mo = _messageOwners.Where(x => x.IsOwner(type));

            foreach (var owner in mo)
            {
                var rma = RabbitMQAddress.From(owner.Endpoint);
                if (string.IsNullOrEmpty(rma.Exchange))
                    throw new MessagePublicationException(
                        $"Cannot publish message without an exchange for {type}: {owner.Endpoint}");

                yield return owner.Endpoint;
            }
        }

        public void RemoveLocalInstanceSubscription(IMessageConsumer consumer)
        {
            throw new NotSupportedException(
                "Instance subscriptions are not supported with this RabbitMQ implementation");
        }

        public object[] GetInstanceSubscriptions(Type type)
        {
            return new object[0];
        }

        public event Action SubscriptionChanged;

        public bool AddSubscription(string type, string endpoint)
        {
            // this is handled by the RabbitMQSubscribeAction
            RaiseSubscriptionChanged();
            return true;
        }

        public void RemoveSubscription(string type, string endpoint)
        {
            // this is handled by the RabbitMQSubscribeAction
            RaiseSubscriptionChanged();
        }

        public void AddLocalInstanceSubscription(IMessageConsumer consumer)
        {
            throw new NotSupportedException(
                "Instance subscriptions are not supported with this RabbitMQ implementation");
        }

        private void RaiseSubscriptionChanged()
        {
            var copy = SubscriptionChanged;
            if (copy != null)
                copy();
        }
    }
}