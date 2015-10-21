using System;
using System.Reactive.Concurrency;
using System.Runtime.InteropServices;

using Confluent.RestClient.Model;

namespace Kafka.Rx.NET.Console
{
    public class Program
    {
        [DllImport("Kernel32")]
        private static extern bool SetConsoleCtrlHandler(EventHandler handler, bool add);
        private delegate bool EventHandler(CtrlType sig);

        static EventHandler _handler;
        static IDisposable _observable;
        private static long _iterations = 0L;

        static void Main(string[] args)
        {
            _handler += Handler;
            SetConsoleCtrlHandler(_handler, true);

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
                    _observable = consumer.GetRecordStream(
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
                    _observable.Dispose();
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
        #region Catch Break.
        private static bool Handler(CtrlType sig)
        {
            Log("Exiting...\r\n");
            if (_observable != null)
            {
                _observable.Dispose();
            }
            return true;
            //            switch (sig)
            //            {
            //                case CtrlType.CTRL_C_EVENT:
            //                case CtrlType.CTRL_LOGOFF_EVENT:
            //                case CtrlType.CTRL_SHUTDOWN_EVENT:
            //                case CtrlType.CTRL_CLOSE_EVENT:
            //                default:
            //                    return false;
            //            }
        }

        enum CtrlType
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT = 1,
            CTRL_CLOSE_EVENT = 2,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT = 6
        }
        #endregion
    }
}
