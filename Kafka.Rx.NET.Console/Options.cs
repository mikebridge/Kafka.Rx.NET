using System;
using CommandLine;
using CommandLine.Text;

namespace Kafka.Rx.NET.Console
{
    public class Options
    {

        [Option('u', "baseurl", Required = true, HelpText = "Kafka Base Url.")]
        public string BaseUrl { get; set; }

        [Option('i', "instanceid", Required = true, HelpText = "The Instance Id in the Consumer Group")]
        public string InstanceId { get; set; }

        [Option('g', "consumergroup", Required = true, HelpText = "The Consumer Group")]
        public string ConsumerGroup { get; set; }

        [Option('t', "topic", Required = true, HelpText = "The Topic")]
        public string Topic { get; set; }

        [Option('s', "sleep", HelpText = "Sleep time between api calls in milliseconds", DefaultValue = 5000)]
        public int Sleep { get; set; }

        [ParserState]
        public IParserState LastParserState { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this, current => HelpText.DefaultParsingErrorsHandler(this, current));
        }

    }
}
