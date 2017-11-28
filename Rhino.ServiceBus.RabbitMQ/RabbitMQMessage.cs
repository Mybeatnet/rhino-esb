using System;
using System.Collections.Generic;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQMessage
    {        
        public byte[] Data { get; set; }
        public Guid MessageId { get; set; }
        public int Priority { get; set; }
        public string ReplyTo { get; set; }
        public IDictionary<string, object> Headers { get; set; }
        public int? Expiration { get; set; }
    }
}