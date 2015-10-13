using System;
using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET.Console
{
    public static class Setup
    {

        public class KafkaClientSettings : IConfluentClientSettings
        {
            public string KafkaBaseUrl { get; set; }
        }

        public static IConfluentClient CreateConfluentClient(String baseUrl)
        {
            var settings = new KafkaClientSettings
            {
                KafkaBaseUrl = baseUrl
            };
            IConfluentClient client = new ConfluentClient(settings);
            return client;
        }

        public static async Task<ConfluentResponse<ConsumerInstance>> CreateConsumerInstance(IConfluentClient client,
           string instanceId, string consumerGroupName)
        {

            var request = new CreateConsumerRequest
            {
                AutoCommitEnabled = true,
                InstanceId = instanceId,
                MessageFormat = MessageFormat.Avro
            };

            return await client.CreateConsumerAsync(consumerGroupName, request);
        }

        public static ConsumerInstance ConsumerInstance(IConfluentClient client, string instanceId, string consumerGroupName)
        {
            var consumerInstanceTask = Setup.CreateConsumerInstance(client, instanceId, consumerGroupName).Result;

            if (!consumerInstanceTask.IsSuccess())
            {
                throw new ApplicationException("Error " + consumerInstanceTask.Error.ErrorCode + ": " +
                                               consumerInstanceTask.Error.Message);
            }
            var consumerInstance = consumerInstanceTask.Payload;
            return consumerInstance;
        }
    }
}
