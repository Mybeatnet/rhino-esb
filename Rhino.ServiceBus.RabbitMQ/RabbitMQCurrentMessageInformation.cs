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

    public class RabbitMQMessage
    {
        public IBasicProperties Properties { get; set; }
        public byte[] Data { get; set; }
    }
    public interface IMsmqTransportAction
    {
        void Init(IMsmqTransport transport, OpenedQueue queue);

        bool CanHandlePeekedMessage(Message message);
        bool HandlePeekedMessage(IMsmqTransport transport, OpenedQueue queue, Message message);
    }
}