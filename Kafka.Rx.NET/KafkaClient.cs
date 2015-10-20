using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET
{
    /// <summary>
    /// Convenience method around IClientCreation
    /// </summary>
    public class KafkaClient
    {
        public static KafkaClient Create()
        {
            return new KafkaClient();
        }

        public KafkaClient WithBaseUrl(String baseUrl)
        {
            BaseUrl = baseUrl;
            return this;
        }

        public String BaseUrl { 
            get;
            private set;
        }

        internal IConfluentClient CreateConfluentClient(String baseUrl)
        {
            var settings = new KafkaClientSettings
            {
                KafkaBaseUrl = baseUrl,
            };
            IConfluentClient client = new ConfluentClient(settings);
            return client;
        }

        // TODO: Is this required?
        internal class KafkaClientSettings : IConfluentClientSettings
        {
            public string KafkaBaseUrl { get; set; }
        }


        // TODO: Move this out of here

//        public static IConfluentClient CreateConfluentClient(String baseUrl)
//        {
//            var settings = new KafkaClientSettings
//            {
//                KafkaBaseUrl = baseUrl
//            };
//            IConfluentClient client = new ConfluentClient(settings);
//            return client;
//        }

    }
}
