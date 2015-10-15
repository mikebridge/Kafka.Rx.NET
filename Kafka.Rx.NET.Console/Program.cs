using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET.Console
{
    public class Program
    {
        private static long _iterations = 0L;

        static void Main(string[] args)
        {
            var options = new Options();

            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                Log("Listening to " + options.BaseUrl+"\r\n");
                Listen(options);

            }
            else
            {
                System.Console.WriteLine(options.GetUsage());
            }
        }

        private static void Log(String msg)
        {
            _iterations++;
            System.Console.Write(msg);  
        }

        private static void Listen(Options options)
        {

            // Arrange
            String instanceId = options.InstanceId;
            string consumerGroupName = options.ConsumerGroup;
            string topic = options.Topic;
            ConsumerInstance consumerInstance = null;

            Log("Registering group/id: " + options.ConsumerGroup + "/" + options.InstanceId+"\r\n");
            Log("Listening to topic: " + topic + "\r\n");
            Log("Query interval: " + options.Sleep + "ms" + "\r\n");
            using (var client = Setup.CreateConfluentClient(options.BaseUrl))
            {
                try
                {
                    // in production this should be written without blocking.
                    consumerInstance = Setup.ConsumerInstance(client, instanceId, consumerGroupName);
                    var consumer = new RxConsumer<String, LogMessage>(client, consumerInstance, topic);

                    // Act
                    var observable = consumer.GetRecordStream(
                            TimeSpan.FromMilliseconds(options.Sleep), 
                            ThreadPoolScheduler.Instance,
                            beforeCallAction: () => Log("."))
                        .Subscribe(
                            // OnSuccess
                            successResult =>
                            {

                                Log("Success: " + successResult.IsSuccess + "\r\n");
                                if (successResult.IsSuccess)
                                {
                                    System.Console.WriteLine(successResult.Value.Key + "=" +
                                                             successResult.Value.Value.Message);
                                }
                                else
                                {
                                    System.Console.WriteLine("ERROR: " + successResult.Exception.Message);
                                }
                            },
                                // OnCompleted
                                () => System.Console.WriteLine("COMPLETED.") // not sure how to cancel this...
                            );

                    System.Console.ReadLine();
                    System.Console.WriteLine("Disposing observer");
                    observable.Dispose();
                }
                catch (Exception ex)
                {
                    System.Console.WriteLine(ex.Message);
                    if (ex.InnerException != null)
                    {
                        System.Console.WriteLine(ex.InnerException.Message);
                    }
                }
                finally
                {
                    if (consumerInstance != null)
                    {
                        Log("Destroying Consumer Instance\r\n");
                        client.DeleteConsumerAsync(consumerInstance);
                        Log("Iterations: " + _iterations);
                    }
                }
            }
        }

    }
}
