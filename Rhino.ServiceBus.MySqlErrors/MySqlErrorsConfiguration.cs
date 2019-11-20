using Rhino.ServiceBus.Impl;

namespace Rhino.ServiceBus.MySqlErrors
{
    public static class MySqlErrorsConfiguration
    {
        public static TConfig UseMySqlErrors<TConfig>(this TConfig self)
            where TConfig : AbstractRhinoServiceBusConfiguration
        {
            self.AddMessageModule<MySqlErrorsMessageModule>();
            self.AddAssemblyContaining<MySqlErrorsMessageModule>();
            return self;
        }
    }
}