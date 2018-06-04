using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Messages;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQMessageBuilder : IMessageBuilder<RabbitMQMessage>
    {
        private readonly IMessageSerializer _messageSerializer;
        private readonly ICustomizeOutgoingMessages[] _customizeHeaders;
        private Endpoint _endpoint;

        public RabbitMQMessageBuilder(IMessageSerializer messageSerializer, IServiceLocator serviceLocator)
        {
            _messageSerializer = messageSerializer;
            _customizeHeaders = serviceLocator.ResolveAll<ICustomizeOutgoingMessages>().ToArray();
        }

        public event Action<RabbitMQMessage> MessageBuilt;

        public RabbitMQMessage BuildFromMessageBatch(OutgoingMessageInformation messageInformation)
        {
            var isAdmin = messageInformation.Messages.Any(x => x is AdministrativeMessage);

            var message = new RabbitMQMessage();

            message.Data = Serialize(messageInformation.Messages);
            message.MessageId = Guid.NewGuid();
            message.Priority = (isAdmin ? 5 : MapPriority(messageInformation.Priority));
            message.ReplyTo = _endpoint.Uri.ToString();
            message.Headers = GetHeaders(messageInformation);
            message.Headers["MessageType"] = (int)GetMessageType(messageInformation.Messages);
            if (messageInformation.DeliverBy.HasValue)
            {
                var timeToDelivery = DateTime.Now - messageInformation.DeliverBy.Value;
                message.Expiration = timeToDelivery > TimeSpan.Zero
                    ? (int) timeToDelivery.TotalMilliseconds
                    : 0;
            }

            MessageBuilt?.Invoke(message);

            return message;
        }

        private int MapPriority(RhinoMessagePriority priority)
        {
            switch (priority)
            {
                case RhinoMessagePriority.Lowest:
                    return 0;
                case RhinoMessagePriority.VeryLow:
                    return 1;
                case RhinoMessagePriority.Low:
                    return 2;
                case RhinoMessagePriority.Normal:
                    return 3;
                case RhinoMessagePriority.AboveNormal:
                    return 4;
                case RhinoMessagePriority.High:
                    return 5;
                case RhinoMessagePriority.VeryHigh:
                    return 6;
                case RhinoMessagePriority.Highest:
                    return 7;
                default:
                    throw new ArgumentOutOfRangeException(nameof(priority), priority, null);
            }
        }

        private IDictionary<string, object> GetHeaders(OutgoingMessageInformation messageInformation)
        {
            messageInformation.Headers = new NameValueCollection();
            foreach (var customizeHeader in _customizeHeaders)
                customizeHeader.Customize(messageInformation);

            return messageInformation.Headers.AllKeys.ToDictionary(x => x, x => (object) messageInformation.Headers[x]);
        }

        private byte[] Serialize(object[] messages)
        {
            using (var ms = new MemoryStream())
            {
                _messageSerializer.Serialize(messages, ms);
                return ms.ToArray();
            }
        }

        private static MessageType GetMessageType(object[] msgs)
        {
            var msg = msgs[0];
            if (msg is AdministrativeMessage)
                return MessageType.AdministrativeMessageMarker;
            if (msg is LoadBalancerMessage)
                return MessageType.LoadBalancerMessageMarker;
            return 0;
        }


        public void Initialize(Endpoint source)
        {
            _endpoint = source;
        }
    }
}