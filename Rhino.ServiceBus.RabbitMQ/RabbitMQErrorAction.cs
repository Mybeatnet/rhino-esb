using System;
using Rhino.ServiceBus.DataStructures;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQErrorAction
    {
        private readonly RabbitMQTransport _transport;
        private readonly Hashtable<string, ErrorCounter> _failureCounts = new Hashtable<string, ErrorCounter>();
        private readonly int _numberOfRetries;

        public RabbitMQErrorAction(int numberOfRetries, RabbitMQTransport transport)
        {
            this._numberOfRetries = numberOfRetries;
            _transport = transport;
        }

        public void Init()
        {
            _transport.MessageSerializationException += Transport_OnMessageSerializationException;
            _transport.MessageProcessingFailure += Transport_OnMessageProcessingFailure;
            _transport.MessageProcessingCompleted += Transport_OnMessageProcessingCompleted;
            _transport.MessageArrived += Transport_OnMessageArrived;
        }

        private bool Transport_OnMessageArrived(CurrentMessageInformation information)
        {
            var info = (RabbitMQCurrentMessageInformation) information;
            ErrorCounter val = null;
            _failureCounts.Read(reader => reader.TryGetValue(info.TransportMessageId, out val));
            if ((val == null) || (val.FailureCount < _numberOfRetries))
                return false;

            var result = false;
            _failureCounts.Write(writer =>
            {
                if (writer.TryGetValue(info.TransportMessageId, out val) == false)
                    return;

                if (val.ExceptionText != null)
                    info.TransportMessage.Headers["error"] = val.ExceptionText;
                info.TransportMessage.Headers["retries"] = val.FailureCount;

                _transport.SendToErrorQueue(info.TransportMessage);

                result = true;
            });

            return result;
        }

        private void Transport_OnMessageSerializationException(CurrentMessageInformation information,
            Exception exception)
        {
            var info = (RabbitMQCurrentMessageInformation) information;
            _failureCounts.Write(writer => writer.Add(info.TransportMessageId, new ErrorCounter
            {
                ExceptionText = exception == null ? null : exception.ToString(),
                FailureCount = _numberOfRetries + 1
            }));

            info.TransportMessage.Headers["retries"] = 1;
            info.TransportMessage.Headers["error"] = exception.ToString();
            _transport.SendToErrorQueue(info.TransportMessage);
        }

        private void Transport_OnMessageProcessingCompleted(CurrentMessageInformation information, Exception ex)
        {
            if (ex != null)
                return;

            ErrorCounter val = null;
            _failureCounts.Read(reader => reader.TryGetValue(information.TransportMessageId, out val));
            if (val == null)
                return;
            _failureCounts.Write(writer => writer.Remove(information.TransportMessageId));
        }

        private void Transport_OnMessageProcessingFailure(CurrentMessageInformation information, Exception exception)
        {
            _failureCounts.Write(writer =>
            {
                ErrorCounter errorCounter;
                if (writer.TryGetValue(information.TransportMessageId, out errorCounter) == false)
                {
                    errorCounter = new ErrorCounter
                    {
                        ExceptionText = exception == null ? null : exception.ToString(),
                        FailureCount = 0
                    };
                    writer.Add(information.TransportMessageId, errorCounter);
                }
                errorCounter.FailureCount += 1;
            });
        }

        private class ErrorCounter
        {
            public string ExceptionText;
            public int FailureCount;
        }
    }
}