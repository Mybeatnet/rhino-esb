using System;
using RabbitMQ.Client;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQCurrentMessageInformation : CurrentMessageInformation
    {
        public Uri ListenUri { get; set; }
        public IModel Model { get; set; }
        public RabbitMQMessage TransportMessage { get; set; }
    }
}