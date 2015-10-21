using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;
using Newtonsoft.Json;

namespace Kafka.Rx.NET
{
    public static class FormatAdapters
    {
        /// <summary>
        /// This assumes that a topic has only one type.  Jay Kreps recommends separating different event types 
        /// into different topics at http://www.confluent.io/blog/stream-data-platform-2/.  Besides, heterogeneous-event 
        /// topics aren't directly supported by the driver.
        /// </summary>
        /// <returns></returns>
        public static async Task<Try<IEnumerable<Record<TK, TV>>>> ConsumeOnceAsAvroAsync<TK, TV>(
            IConfluentClient confluentClient,
            ConsumerInstance consumerInstance,
            string topic) where TK : class where TV : class
        {
            try
            {
                var result = await confluentClient.ConsumeAsAvroAsync<TK, TV>(consumerInstance, topic);
                if (result.IsSuccess())
                {
                    return new Success<IEnumerable<Record<TK, TV>>>(result.Payload.Select(
                        record => (new Record<TK, TV>(record.Key, record.Value))));
                }
                else
                {
                    return new Failure<IEnumerable<Record<TK, TV>>>(
                            new Exception(result.Error.ErrorCode + ": " + result.Error.Message));
                }
            }
            catch (Exception ex)
            {
                return new Failure<IEnumerable<Record<TK, TV>>>(ex);

            }

        }

        public static async Task<Try<IEnumerable<Record<TK, TV>>>> ConsumeOnceAsBinaryAsync<TK, TV>(
                    IConfluentClient confluentClient,
                    ConsumerInstance consumerInstance,
                    string topic)
            where TK : class
            where TV : class
        {
            try
            {
                var result = await confluentClient.ConsumeAsBinaryAsync(consumerInstance, topic /*, maxBytes: 10000*/);
                if (result.IsSuccess())
                {
                    return new Success<IEnumerable<Record<TK, TV>>>(
                        result.Payload.Select(
                            record => (new Record<TK, TV>(null, BinaryConverters.FromBinaryJson<TV>(record.Value)))));
                }
                else
                {
                    return new Failure<IEnumerable<Record<TK, TV>>>(
                        new Exception(result.Error.ErrorCode + ": " + result.Error.Message));
                }
            }
            catch (Exception ex)
            {
                return new Failure<IEnumerable<Record<TK, TV>>>(ex);

            }
        }
    }
}
