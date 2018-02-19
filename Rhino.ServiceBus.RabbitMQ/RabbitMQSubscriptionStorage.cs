using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using Rhino.ServiceBus.DataStructures;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.MessageModules;
using Rhino.ServiceBus.Messages;
using Rhino.ServiceBus.Transport;
using Rhino.ServiceBus.Util;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQSubscriptionStorage : ISubscriptionStorage, IMessageModule
    {
        private readonly RabbitMQConnectionProvider _connectionProvider;

        private readonly Hashtable<string, List<WeakReference>> _localInstanceSubscriptions =
            new Hashtable<string, List<WeakReference>>();

        private readonly ILog _logger = LogManager.GetLogger(typeof(RabbitMQSubscriptionStorage));
        private readonly IMessageSerializer _messageSerializer;

        private readonly IReflection _reflection;

        private readonly MultiValueIndexHashtable<Guid, string, Uri, string> _remoteInstanceSubscriptions =
            new MultiValueIndexHashtable<Guid, string, Uri, string>();

        private readonly Hashtable<TypeAndUriKey, IList<string>> _subscriptionMessageIds =
            new Hashtable<TypeAndUriKey, IList<string>>();

        private readonly RabbitMQAddress _subscriptionQueue;

        private readonly Hashtable<string, HashSet<Uri>> _subscriptions = new Hashtable<string, HashSet<Uri>>();

        public RabbitMQSubscriptionStorage(
            IReflection reflection,
            IMessageSerializer messageSerializer,
            Uri queueBusListensTo,
            RabbitMQConnectionProvider connectionProvider)
        {
            _reflection = reflection;
            _messageSerializer = messageSerializer;
            _connectionProvider = connectionProvider;
            _subscriptionQueue = RabbitMQAddress.From(queueBusListensTo);            
            _subscriptionQueue.QueueName += ".subscriptions";
        }

        void IMessageModule.Init(ITransport transport, IServiceBus bus)
        {
            transport.AdministrativeMessageArrived += HandleAdministrativeMessage;
        }

        void IMessageModule.Stop(ITransport transport, IServiceBus bus)
        {
            transport.AdministrativeMessageArrived -= HandleAdministrativeMessage;
        }

        public void Initialize()
        {
            _logger.DebugFormat("Initializing rabbitmq subscription storage on: {0}", _subscriptionQueue);
            using (var model = _connectionProvider.Open(_subscriptionQueue, true))
            {
                while (true)
                {
                    var current = model.BasicGet(_subscriptionQueue.QueueName, false);
                    if (current == null)
                        break;

                    var rmsg = new RabbitMQMessage(current.Body, current.BasicProperties);
                    object[] msgs;
                    try
                    {
                        msgs = _messageSerializer.Deserialize(current.Body);
                    }
                    catch (Exception e)
                    {
                        throw new SubscriptionException("Could not deserialize message from subscription queue", e);
                    }

                    try
                    {
                        foreach (var msg in msgs)
                            HandleAdministrativeMessage(new RabbitMQCurrentMessageInformation(rmsg)
                            {
                                AllMessages = msgs,
                                Message = msg,
                                TransportMessageId = current.BasicProperties.MessageId,
                                Source = _subscriptionQueue.ToUri()
                            });
                    }
                    catch (Exception e)
                    {
                        throw new SubscriptionException("Failed to process subscription records", e);
                    }
                }
            }
        }

        public IEnumerable<Uri> GetSubscriptionsFor(Type type)
        {
            HashSet<Uri> subscriptionForType = null;
            _subscriptions.Read(reader => reader.TryGetValue(type.FullName, out subscriptionForType));
            var subscriptionsFor = subscriptionForType ?? new HashSet<Uri>();

            List<Uri> instanceSubscriptions;
            _remoteInstanceSubscriptions.TryGet(type.FullName, out instanceSubscriptions);

            subscriptionsFor.UnionWith(instanceSubscriptions);

            return subscriptionsFor;
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
            var added = false;
            _subscriptions.Write(writer =>
            {
                HashSet<Uri> subscriptionsForType;
                if (writer.TryGetValue(type, out subscriptionsForType) == false)
                {
                    subscriptionsForType = new HashSet<Uri>();
                    writer.Add(type, subscriptionsForType);
                }

                var uri = new Uri(endpoint);
                added = subscriptionsForType.Add(uri);

                _logger.InfoFormat("Added subscription for {0} on {1}",
                    type, uri);
            });

            RaiseSubscriptionChanged();
            return added;
        }

        public void RemoveSubscription(string type, string endpoint)
        {
            var uri = new Uri(endpoint);
            var radr = RabbitMQAddress.FromString(endpoint);
            using (var model = _connectionProvider.Open(radr, true))
            {
                var msgIds = RemoveSubscriptionMessageFromQueue(type, uri);
                while (true)
                {
                    var current = model.BasicGet(radr.QueueName, false);
                    if (current == null)
                        break;

                    if (msgIds.Contains(current.BasicProperties.MessageId))
                        model.BasicAck(current.DeliveryTag, false);
                }
            }

            _subscriptions.Write(writer =>
            {
                HashSet<Uri> subscriptionsForType;

                if (writer.TryGetValue(type, out subscriptionsForType) == false)
                {
                    subscriptionsForType = new HashSet<Uri>();
                    writer.Add(type, subscriptionsForType);
                }

                subscriptionsForType.Remove(uri);

                _logger.InfoFormat("Removed subscription for {0} on {1}",
                    type, endpoint);
            });

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

        private void AddMessageIdentifierForTracking(string messageId, string messageType, Uri uri)
        {
            _subscriptionMessageIds.Write(writer =>
            {
                var key = new TypeAndUriKey {TypeName = messageType, Uri = uri};
                IList<string> value;
                if (writer.TryGetValue(key, out value) == false)
                {
                    value = new List<string>();
                    writer.Add(key, value);
                }

                if (string.IsNullOrEmpty(messageId))
                    throw new ArgumentException("messageId must have value");

                value.Add(messageId);
            });
        }

        private IList<string> RemoveSubscriptionMessageFromQueue(string type, Uri uri)
        {
            IList<string> messageIds = new List<string>();
            _subscriptionMessageIds.Write(writer =>
            {
                var key = new TypeAndUriKey
                {
                    TypeName = type,
                    Uri = uri
                };

                if (writer.TryGetValue(key, out messageIds) == false)
                    return;
                writer.Remove(key);
            });

            return messageIds;
        }

        public bool HandleAdministrativeMessage(CurrentMessageInformation msgInfo)
        {
            var addSubscription = msgInfo.Message as AddSubscription;
            if (addSubscription != null)
                return ConsumeAddSubscription(msgInfo, addSubscription);
            var removeSubscription = msgInfo.Message as RemoveSubscription;
            if (removeSubscription != null)
                return ConsumeRemoveSubscription(removeSubscription);
            var addInstanceSubscription = msgInfo.Message as AddInstanceSubscription;
            if (addInstanceSubscription != null)
                return ConsumeAddInstanceSubscription(msgInfo, addInstanceSubscription);
            var removeInstanceSubscription = msgInfo.Message as RemoveInstanceSubscription;
            if (removeInstanceSubscription != null)
                return ConsumeRemoveInstanceSubscription(removeInstanceSubscription);
            return false;
        }

        private bool ConsumeRemoveInstanceSubscription(RemoveInstanceSubscription subscription)
        {
            string msgId;
            if (_remoteInstanceSubscriptions.TryRemove(subscription.InstanceSubscriptionKey, out msgId))
            {
                using (var model = _connectionProvider.Open(_subscriptionQueue, true))
                {
                    while (true)
                    {
                        var current = model.BasicGet(_subscriptionQueue.QueueName, false);
                        if (current == null)
                            break;

                        if (msgId == current.BasicProperties.MessageId)
                            model.BasicAck(current.DeliveryTag, false);
                    }
                }

                RaiseSubscriptionChanged();
            }
            return true;
        }

        private bool ConsumeAddInstanceSubscription(CurrentMessageInformation msgInfo,
            AddInstanceSubscription subscription)
        {
            var rmqMsgInfo = msgInfo as RabbitMQCurrentMessageInformation;
            var msgId = msgInfo.TransportMessageId;
            if (rmqMsgInfo != null)
                SendToSubscriptionQueue(rmqMsgInfo);

            _remoteInstanceSubscriptions.Add(
                subscription.InstanceSubscriptionKey,
                subscription.Type,
                new Uri(subscription.Endpoint),
                msgId);
            RaiseSubscriptionChanged();
            return true;
        }

        private void SendToSubscriptionQueue(RabbitMQCurrentMessageInformation rmqMsgInfo)
        {
            var props = rmqMsgInfo.Model.CreateBasicProperties();
            rmqMsgInfo.TransportMessage.Populate(props);
            rmqMsgInfo.Model.BasicPublish(_subscriptionQueue.Exchange, "", true, props, rmqMsgInfo.TransportMessage.Data);
        }

        private bool ConsumeRemoveSubscription(RemoveSubscription removeSubscription)
        {
            RemoveSubscription(removeSubscription.Type, removeSubscription.Endpoint.Uri.ToString());
            return true;
        }

        private bool ConsumeAddSubscription(CurrentMessageInformation msgInfo, AddSubscription addSubscription)
        {
            var newSubscription = AddSubscription(addSubscription.Type, addSubscription.Endpoint.Uri.ToString());

            var rmqMsgInfo = msgInfo as RabbitMQCurrentMessageInformation;

            if ((rmqMsgInfo != null) && newSubscription)
            {
                SendToSubscriptionQueue(rmqMsgInfo);

                AddMessageIdentifierForTracking(
                    rmqMsgInfo.TransportMessageId,
                    addSubscription.Type,
                    addSubscription.Endpoint.Uri);

                return true;
            }

            AddMessageIdentifierForTracking(
                msgInfo.TransportMessageId,
                addSubscription.Type,
                addSubscription.Endpoint.Uri);
            return false;
        }

        private void RaiseSubscriptionChanged()
        {
            var copy = SubscriptionChanged;
            if (copy != null)
                copy();
        }
    }
}