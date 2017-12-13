using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQMessage
    {
        public RabbitMQMessage(byte[] data, IBasicProperties properties)
        {
            Data = data;
            MessageId = Guid.Parse(properties.MessageId);
            Priority = properties.Priority;
            ReplyTo = properties.ReplyTo;
            Headers = properties.Headers;
        }

        public RabbitMQMessage(BasicDeliverEventArgs arg) : this(arg.Body, arg.BasicProperties)
        {
        }

        public RabbitMQMessage()
        {            
        }

        public byte[] Data { get; set; }
        public Guid MessageId { get; set; }
        public int Priority { get; set; }
        public string ReplyTo { get; set; }
        public IDictionary<string, object> Headers { get; set; }
        public int? Expiration { get; set; }

        public void Populate(IBasicProperties props)
        {
            props.MessageId = MessageId.ToString();
            props.Priority = (byte)Priority;            
            props.ReplyTo = ReplyTo;
            if (Expiration != null)
                props.Expiration = Expiration.ToString();
            props.Headers = Headers;
            props.DeliveryMode = 2; // persistent
        }
    }
}