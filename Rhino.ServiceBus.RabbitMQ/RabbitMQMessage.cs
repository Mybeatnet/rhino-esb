using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQMessage
    {
        public RabbitMQMessage(byte[] data, IBasicProperties properties)
        {
            Data = data;
            MessageId = Guid.Parse(properties.MessageId);
            Priority = properties.Priority;
            ReplyTo = properties.ReplyTo;
            Headers = properties.Headers.ToDictionary(x => x.Key, x => GetHeaderValue(x.Value));
        }

        /// <summary>
        /// RabbitMQ client interprates strings in headers as byte[], so we need to transform them back to strings
        /// This does mean that sending byte[] in a header is not going to work - could use Base64 in that case
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        private object GetHeaderValue(object source)
        {
            if (source is byte[]) // RabbitMQ sends strings back as byte[] arrays
                return Encoding.UTF8.GetString((byte[]) source);
            return source;
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