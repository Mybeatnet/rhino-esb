using System;
using Rhino.ServiceBus.DataStructures;
using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
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
        }

        /// <summary>
        /// Returns true if message is processed by error action, and should not be processed further
        /// </summary>
        /// <param name="information"></param>
        /// <returns></returns>
        public bool Process(CurrentMessageInformation information)
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
                info.TransportMessage.Headers["failures"] = val.FailureCount;

                _transport.SendToErrorQueue(info.TransportMessage);

                writer.Remove(info.TransportMessageId);

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
                ExceptionText = exception?.ToString(),
                FailureCount = _numberOfRetries + 1
            }));
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