using System;
using RabbitMQ.Client;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class OpenedSession : IDisposable
    {
        private readonly IModel _model;
        private int _refs;

        public OpenedSession(IConnection connection, IModel model)
        {
            Connection = connection;
            _model = model;
        }

        public bool IsActive => _refs > 0;

        public IConnection Connection { get; }

        public void Dispose()
        {
            if (--_refs == 0)
            {
                _model.Close(200, "Ok");
                _model.Dispose();
                Connection.Close();
                Connection.Dispose();
                Disposed?.Invoke(this, EventArgs.Empty);
            }
        }

        public event EventHandler Disposed;

        public OpenedSession AddRef()
        {
            _refs++;
            return this;
        }

        public IModel Model()
        {
            return _model;
        }
    }
}