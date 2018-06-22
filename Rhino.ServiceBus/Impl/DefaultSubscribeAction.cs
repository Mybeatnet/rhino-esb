using System;
using Common.Logging;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Messages;

namespace Rhino.ServiceBus.Impl
{
    public class DefaultSubscribeAction : ISubscribeAction
    {
        private readonly ILog _logger = LogManager.GetLogger(typeof(DefaultSubscribeAction));
        private IServiceBus _bus;
        private readonly IEndpointRouter _endpointRouter;
        private readonly MessageOwnersSelector _messageOwners;

        public DefaultSubscribeAction(IEndpointRouter endpointRouter, MessageOwnersSelector messageOwners)
        {
            _endpointRouter = endpointRouter;
            _messageOwners = messageOwners;
        }

        public void Unsubscribe(Type type, Endpoint endpoint)
        {
            _bus.Send(endpoint, new RemoveSubscription
            {
                Endpoint = _bus.Endpoint,
                Type = type.FullName
            });
        }

        public void Init(IServiceBus bus)
        {
            _bus = bus;
        }

        public void Subscribe(Type type, Endpoint endpoint)
        {
            _bus.Send(endpoint, new AddSubscription
            {
                Endpoint = _bus.Endpoint,
                Type = type.FullName
            });
        }

        public void SubscribeInstanceSubscription(InstanceSubscriptionInformation information)
        {
            foreach (var message in information.ConsumedMessages)
            {
                bool subscribed = false;
                foreach (var owner in _messageOwners.Of(message))
                {
                    _logger.DebugFormat("Instance subscribition for {0} on {1}",
                        message.FullName,
                        owner.Endpoint);

                    subscribed = true;
                    var endpoint = _endpointRouter.GetRoutedEndpoint(owner.Endpoint);
                    endpoint.Transactional = owner.Transactional;
                    _bus.Send(endpoint, new AddInstanceSubscription
                    {
                        Endpoint = _bus.Endpoint.Uri.ToString(),
                        Type = message.FullName,
                        InstanceSubscriptionKey = information.InstanceSubscriptionKey
                    });
                }

                if (subscribed == false)
                    throw new SubscriptionException("Could not find any owner for message " + message +
                                                    " that we could subscribe for");
            }
        }

        public void UnsubscribeInstanceSubscription(InstanceSubscriptionInformation information)
        {
            foreach (var message in information.ConsumedMessages)
            {
                foreach (var owner in _messageOwners.Of(message))
                {
                    var endpoint = _endpointRouter.GetRoutedEndpoint(owner.Endpoint);
                    endpoint.Transactional = owner.Transactional;
                    _bus.Send(endpoint, new RemoveInstanceSubscription
                    {
                        Endpoint = _bus.Endpoint.Uri.ToString(),
                        Type = message.FullName,
                        InstanceSubscriptionKey = information.InstanceSubscriptionKey
                    });
                }
            }
        }
    }
}