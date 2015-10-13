using System;
using System.Runtime.Serialization;

namespace Kafka.Rx.NET.Console
{
    [DataContract]
    public class LogMessage
    {
        [DataMember(Name = "message")]
        public String Message { get; set; }

        [DataMember(Name = "stacktrace")]
        public String StackTrace { get; set; }
    }
}
