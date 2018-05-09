using System;
using System.Collections.Generic;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Internal;

namespace Rhino.ServiceBus.Impl
{
    public class DefaultPublishAction : IPublishAction
    {
        private readonly IEndpointRouter endpointRouter;
        private readonly ISubscriptionStorage subscriptionStorage;
        private readonly ITransport transport;

        public DefaultPublishAction(ITransport transport, ISubscriptionStorage subscriptionStorage,
            IEndpointRouter endpointRouter)
        {
            this.transport = transport;
            this.subscriptionStorage = subscriptionStorage;
            this.endpointRouter = endpointRouter;
        }

        public bool Publish(object[] messages)
        {
            if (messages == null)
                throw new ArgumentNullException("messages");

            var sentMsg = false;
            if (messages.Length == 0)
                throw new MessagePublicationException("Cannot publish an empty message batch");

            var subscriptions = new HashSet<Uri>();
            foreach (var message in messages)
            {
                var messageType = message.GetType();
                while (messageType != null)
                {
                    subscriptions.UnionWith(subscriptionStorage.GetSubscriptionsFor(messageType));
                    foreach (var interfaceType in messageType.GetInterfaces())
                        subscriptions.UnionWith(subscriptionStorage.GetSubscriptionsFor(interfaceType));
                    messageType = messageType.BaseType;
                }
            }

            foreach (var subscription in subscriptions)
            {
                transport.Send(endpointRouter.GetRoutedEndpoint(subscription), messages);
                sentMsg = true;
            }

            return sentMsg;
        }
    }
}