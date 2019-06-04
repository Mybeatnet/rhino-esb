using Common.Logging;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.MessageModules;
using System;
using System.Threading;

namespace Rhino.ServiceBus.Inbox
{
    public class RsbInboxMessageModule : IMessageModule
    {
        private static readonly ILog _log = LogManager.GetLogger<RsbInboxMessageModule>();

        private readonly ThreadLocal<RsbInbox> _currentInbox = new ThreadLocal<RsbInbox>();

        private readonly RsbInboxFactory _inboxFactory;
        private IServiceBus _bus;
        private Timer _timer;

        public RsbInboxMessageModule(RsbInboxFactory inboxFactory)
        {
            _inboxFactory = inboxFactory;
        }

        public void Init(ITransport transport, IServiceBus bus)
        {
            _bus = bus;
            if (_inboxFactory.Enabled)
            {
                _inboxFactory.Init(_bus.Endpoint.Uri.AbsolutePath);
                transport.MessageArrived += OnMessageArrived;
                transport.MessageProcessingCompleted += OnMessageProcessingCompleted;
                _timer = new Timer(CleanupInbox, this, GetDueTime(), TimeSpan.FromDays(1));
            }
        }

        public void Stop(ITransport transport, IServiceBus bus)
        {
            if (_inboxFactory.Enabled)
            {
                transport.MessageArrived -= OnMessageArrived;
                transport.MessageProcessingCompleted -= OnMessageProcessingCompleted;
                _timer.Dispose();
            }

            _bus = null;
        }

        public void CleanupInbox(object state)
        {
            try
            {
                using (var cleanup = _inboxFactory.CreateInbox())
                {
                    using (cleanup.GetCleanupLock(out var locked))
                    {
                        if (locked)
                        {
                            if (cleanup.ScheduleCleanup())
                                _bus.SendToSelf(new CleanupInboxMessage
                                {
                                    Date = DateTime.Now.Subtract(_inboxFactory.CleanupAge)
                                });
                            cleanup.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error("Error scheduling inboxFactory cleanup", ex);
            }
        }

        private TimeSpan GetDueTime()
        {
            var dt = DateTime.Today.AddHours(3);
            var due = DateTime.Now - dt;
            if (due.TotalMilliseconds < 0)
                due = due.Add(TimeSpan.FromDays(1));

            return due;
        }

        private bool OnMessageArrived(CurrentMessageInformation cmi)
        {
            var inbox = _inboxFactory.CreateInbox();

            _currentInbox.Value = inbox;

            var succeeded = inbox.Insert(DateTime.Now, cmi.MessageId);

            if (!succeeded)
                return true;

            if (cmi.Message is CleanupInboxMessage cleanupMsg)
            {
                ProcessCleanupInbox(cleanupMsg);
                return true;
            }

            // not handled by message module => continue with processing
            return false;
        }

        private void OnMessageProcessingCompleted(CurrentMessageInformation messageInfo, Exception exception)
        {
            try
            {
                if (exception == null)
                    _currentInbox.Value?.Commit();
            }
            catch (Exception ex)
            {
                _log.Error("Error committing RsbInbox", ex);
                exception = ex;
            }

            try
            {
                if (exception != null)
                    _currentInbox.Value?.Rollback();
            }
            finally
            {
                _currentInbox.Value?.Dispose();                
                _currentInbox.Value = null;
            }
        }

        private void ProcessCleanupInbox(CleanupInboxMessage msg)
        {
            var rows = _currentInbox.Value.Cleanup(msg.Date, _inboxFactory.CleanupRows);
            // if we have processed the same number of rows as requested then there might be more rows to delete
            if (rows == _inboxFactory.CleanupRows)
                _bus.SendToSelf(msg);
        }

        public class CleanupInboxMessage
        {
            public DateTime Date { get; set; }
        }
    }
}