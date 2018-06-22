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
        private readonly Hashtable<string, List<WeakReference>> _localInstanceSubscriptions =
            new Hashtable<string, List<WeakReference>>();

        private readonly ILog _logger = LogManager.GetLogger(typeof(RabbitMQSubscriptionStorage));

        private readonly IEnumerable<MessageOwner> _messageOwners;
        private readonly IReflection _reflection;


        public RabbitMQSubscriptionStorage(IEnumerable<MessageOwner> messageOwners, IReflection reflection)
        {
            _messageOwners = messageOwners;
            _reflection = reflection;
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

        private void RaiseSubscriptionChanged()
        {
            var copy = SubscriptionChanged;
            if (copy != null)
                copy();
        }
    }
}