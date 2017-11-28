using System;
using System.ComponentModel;
using System.Messaging;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Transactions;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Transport;
using MessageType=Rhino.ServiceBus.Transport.MessageType;

namespace Rhino.ServiceBus.Msmq
{
    public static class MsmqExtensions
    {
        private static PropertyInfo _internalTransaction =
            typeof(MessageQueueTransaction).GetProperty("InnerTransaction",
                BindingFlags.Instance | BindingFlags.NonPublic);

        public static string EnsureLabelLength(this string label)
        {
            if (label.Length > 249)
                label = label.Substring(0, 246) + "...";
            return label;
        }

        public static Message SetSubQueueToSendTo(this Message self, SubQueue queue)
        {
            self.AppSpecific = ((int) MessageType.MoveMessageMarker << 16) | (int) queue;
            return self;
        }

        public static Guid GetMessageId(this Message self)
        {
            if (self.Extension.Length < 16)
                throw new InvalidOperationException(
                    "Message is not in a format that the bus can understand, Message's Extension is not a Guid");
            var guid = new byte[16];
            Buffer.BlockCopy(self.Extension, 0, guid, 0, 16);
            return new Guid(guid);
        }

        private static GCHandle? GetTransaction()
        {
            var current = Transaction.Current;
            if (current != null)
            {
                var trans = TransactionInterop.GetDtcTransaction(current);                        
                return GCHandle.Alloc(trans, GCHandleType.Pinned);
            }
            var curMq = MsmqTransactionStrategy.Current;
            if (curMq != null)
            {
                var trans = _internalTransaction.GetValue(curMq, null);
                return GCHandle.Alloc(trans, GCHandleType.Pinned);
            }

            return null;
        }

        public static void MoveToSubQueue(
            this MessageQueue queue,
            string subQueueName,
            Message message)
        {
            var fullSubQueueName = @"DIRECT=OS:.\" + queue.QueueName + ";" + subQueueName;
            IntPtr queueHandle = IntPtr.Zero;
            var error = NativeMethods.MQOpenQueue(fullSubQueueName, NativeMethods.MQ_MOVE_ACCESS,
                NativeMethods.MQ_DENY_NONE, ref queueHandle);
            if (error != 0)
                throw new TransportException("Failed to open queue: " + fullSubQueueName,
                    new Win32Exception(error));

            try
            {
                if (MsmqTransactionStrategy.Current != null)
                {
                    //var trans = _internalTransaction.GetValue(MsmqTransactionStrategy.Current, null);

                    error = NativeMethods.MQMoveMessage(queue.ReadHandle, queueHandle,
                        message.LookupId, null);
                    if (error != 0)
                        throw new TransportException("Failed to move message to queue: " + fullSubQueueName,
                            new Win32Exception(error));

                    return;
                }

                Transaction current = Transaction.Current;
                IDtcTransaction transaction = null;
                if (current != null && queue.Transactional)
                {
                    transaction = TransactionInterop.GetDtcTransaction(current);
                }

                error = NativeMethods.MQMoveMessage(queue.ReadHandle, queueHandle,
                    message.LookupId, transaction);
                if (error != 0)
                    throw new TransportException("Failed to move message to queue: " + fullSubQueueName,
                        new Win32Exception(error));
            }
            finally
            {
                error = NativeMethods.MQCloseQueue(queueHandle);
                if (error != 0)
                    throw new TransportException("Failed to close queue: " + fullSubQueueName,
                        new Win32Exception(error));

            }
        }

        /// <summary>
        /// Gets the count.
        /// http://blog.codebeside.org/archive/2008/08/27/counting-the-number-of-messages-in-a-message-queue-in.aspx
        /// </summary>
        /// <param name="self">The self.</param>
        /// <returns></returns>
        public static int GetCount(this MessageQueue self)
        {
            if (!MessageQueue.Exists(self.MachineName + @"\" + self.QueueName))
            {
                return 0;
            }

            var props = new NativeMethods.MQMGMTPROPS {cProp = 1};
            try
            {
                props.aPropID = Marshal.AllocHGlobal(sizeof(int));
                Marshal.WriteInt32(props.aPropID, NativeMethods.PROPID_MGMT_QUEUE_MESSAGE_COUNT);

                props.aPropVar = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(NativeMethods.MQPROPVariant)));
                Marshal.StructureToPtr(new NativeMethods.MQPROPVariant {vt = NativeMethods.VT_NULL}, props.aPropVar,
                    false);

                props.status = Marshal.AllocHGlobal(sizeof(int));
                Marshal.WriteInt32(props.status, 0);

                int result = NativeMethods.MQMgmtGetInfo(null, "queue=" + self.FormatName, ref props);
                if (result != 0)
                    throw new Win32Exception(result);

                if (Marshal.ReadInt32(props.status) != 0)
                {
                    return 0;
                }

                var propVar =
                    (NativeMethods.MQPROPVariant)
                    Marshal.PtrToStructure(props.aPropVar, typeof(NativeMethods.MQPROPVariant));
                if (propVar.vt != NativeMethods.VT_UI4)
                {
                    return 0;
                }
                else
                {
                    return Convert.ToInt32(propVar.ulVal);
                }
            }
            finally
            {
                Marshal.FreeHGlobal(props.aPropID);
                Marshal.FreeHGlobal(props.aPropVar);
                Marshal.FreeHGlobal(props.status);
            }
        }


        public static void TransactionalSend(this MessageQueue self, Message message, bool transactional)
        {
            if (transactional == false)
            {
                self.Send(message, MessageQueueTransactionType.None);
                return;
            }

            if (Transaction.Current != null)
            {
                self.Send(message, MessageQueueTransactionType.Automatic);
                return;
            }

            var mqt = MsmqTransactionStrategy.Current;
            if (mqt != null)
            {
                self.Send(message, mqt);
                return;
            }

            self.Send(message, MessageQueueTransactionType.Single);
        }

        public static Message TransactionalReceiveById(this MessageQueue self, string id)
        {
            if (Transaction.Current != null)
                return self.ReceiveById(id, MessageQueueTransactionType.Automatic);

            var mqt = MsmqTransactionStrategy.Current;
            if (mqt != null)
                return self.ReceiveById(id, mqt);

            return self.ReceiveById(id, MessageQueueTransactionType.Single);
        }

        public static Message TransactionalReceive(this MessageQueue self)
        {
            if (Transaction.Current != null)
                return self.Receive(MessageQueueTransactionType.Automatic);

            var mqt = MsmqTransactionStrategy.Current;
            if (mqt != null)
                return self.Receive(mqt);

            return self.Receive(MessageQueueTransactionType.Single);
        }

        public static Message TransactionalRemoveCurrent(this MessageEnumerator self)
        {
            if (Transaction.Current != null)
                return self.RemoveCurrent(MessageQueueTransactionType.Automatic);
            var mqt = MsmqTransactionStrategy.Current;
            if (mqt != null)
                return self.RemoveCurrent(mqt);

            return self.RemoveCurrent(MessageQueueTransactionType.Single);
        }
    }
}
