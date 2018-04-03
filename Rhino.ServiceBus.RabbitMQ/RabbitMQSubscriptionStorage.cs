using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using Rhino.ServiceBus.DataStructures;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQSubscriptionStorage : ISubscriptionStorage
    {
        private readonly RabbitMQAddress _busUri;
        private readonly RabbitMQConnectionProvider _connectionProvider;

        private readonly Hashtable<string, List<WeakReference>> _localInstanceSubscriptions =
            new Hashtable<string, List<WeakReference>>();

        private readonly ILog _logger = LogManager.GetLogger(typeof(RabbitMQSubscriptionStorage));
        private readonly IEnumerable<MessageOwner> _messageOwners;

        private readonly IReflection _reflection;

        public RabbitMQSubscriptionStorage(IReflection reflection,
            Uri queueBusListensTo,
            RabbitMQConnectionProvider connectionProvider, IEnumerable<MessageOwner> messageOwners)
        {
            _reflection = reflection;
            _connectionProvider = connectionProvider;
            _messageOwners = messageOwners;
            _busUri = RabbitMQAddress.From(queueBusListensTo);
        }

        public void Initialize()
        {
        }

        public IEnumerable<Uri> GetSubscriptionsFor(Type type)
        {
            var typeName = type.FullName;
            // we have to return the exchange uri to send the message to - this is just the message owner
            var mo = _messageOwners.FirstOrDefault(x => typeName.StartsWith(x.Name));
            if (mo == null)
                throw new MessagePublicationException($"Could not find message owner for {type} in config");

            var rma = RabbitMQAddress.From(mo.Endpoint);
            if (string.IsNullOrEmpty(rma.Exchange))
                throw new MessagePublicationException(
                    $"Cannot publish message without an exchange for {type}: {mo.Endpoint}");

            yield return mo.Endpoint;
        }

        public void RemoveLocalInstanceSubscription(IMessageConsumer consumer)
        {
            var messagesConsumes = _reflection.GetMessagesConsumed(consumer);
            var changed = false;
            var list = new List<WeakReference>();

            _localInstanceSubscriptions.Write(writer =>
            {
                foreach (var type in messagesConsumes)
                {
                    List<WeakReference> value;

                    if (writer.TryGetValue(type.FullName, out value) == false)
                        continue;
                    writer.Remove(type.FullName);
                    list.AddRange(value);
                }
            });

            foreach (var reference in list)
            {
                if (ReferenceEquals(reference.Target, consumer))
                    continue;

                changed = true;
            }

            if (changed)
                RaiseSubscriptionChanged();
        }

        public object[] GetInstanceSubscriptions(Type type)
        {
            List<WeakReference> value = null;

            _localInstanceSubscriptions.Read(reader => reader.TryGetValue(type.FullName, out value));

            if (value == null)
                return new object[0];

            var liveInstances = value
                .Select(x => x.Target)
                .Where(x => x != null)
                .ToArray();

            if (liveInstances.Length != value.Count) //cleanup
                _localInstanceSubscriptions.Write(writer => value.RemoveAll(x => x.IsAlive == false));

            return liveInstances;
        }


        public event Action SubscriptionChanged;

        public bool AddSubscription(string type, string endpoint)
        {
            var broker = RabbitMQAddress.FromString(endpoint);
            if (string.IsNullOrEmpty(broker.Exchange))
                throw new SubscriptionException(
                    $"Cannot subscribe to endpoint without an exchange for {type}: {endpoint}");

            _connectionProvider.BindQueue(broker, broker.Exchange, _busUri.QueueName, type);
            _logger.InfoFormat("Added subscription for {0} on {1}", type, endpoint);
            RaiseSubscriptionChanged();
            return true;
        }

        public void RemoveSubscription(string type, string endpoint)
        {
            var broker = RabbitMQAddress.FromString(endpoint);
            if (string.IsNullOrEmpty(broker.Exchange))
                throw new SubscriptionException(
                    $"Cannot unsubscribe from endpoint without an exchange for {type}: {endpoint}");
            _connectionProvider.UnbindQueue(broker, broker.Exchange, _busUri.QueueName, type);
            _logger.InfoFormat("Removed subscription for {0} on {1}", type, endpoint);

            RaiseSubscriptionChanged();
        }

        public void AddLocalInstanceSubscription(IMessageConsumer consumer)
        {
            _localInstanceSubscriptions.Write(writer =>
            {
                foreach (var type in _reflection.GetMessagesConsumed(consumer))
                {
                    List<WeakReference> value;
                    if (writer.TryGetValue(type.FullName, out value) == false)
                    {
                        value = new List<WeakReference>();
                        writer.Add(type.FullName, value);
                    }

                    value.Add(new WeakReference(consumer));
                }
            });
            RaiseSubscriptionChanged();
        }

        private void RaiseSubscriptionChanged()
        {
            var copy = SubscriptionChanged;
            if (copy != null)
                copy();
        }
    }
}