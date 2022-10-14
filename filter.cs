using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;

namespace ServiceBusSubscriber
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://testuditha.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=qTtT4XY+hwePRcF1nuU8+5lFQS5mxLQm7+jXxcT9FQQ=";
        const string TopicName = "SM-Test";
        static SubscriptionClient subscriptionClient;
        static ManagementClient managementClient;
        static async Task Main(string[] args)
        {
            managementClient = new ManagementClient(ServiceBusConnectionString);

            await CreateTopicIfNotExistsAsync(TopicName);
            await CreateSubscriptionIfNotExistsAsync(TopicName, "EvenNumbers");

            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, "EvenNumbers");
            await RemoveDefaultRuleAsync();

            var isEvenFilter = CreateFilter("IsEven", true);
            var ruleDescription = new RuleDescription("MyFilter", isEvenFilter);
            await AddRuleIfNotExistsAsync(ruleDescription);

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);

            Console.ReadKey();
        }


        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

        static CorrelationFilter CreateFilter(string propertyName, object value)
        {
            var filter = new CorrelationFilter();
            filter.Properties[propertyName] = value;

            return filter;
        }

        static async Task RemoveDefaultRuleAsync()
        {
            var rules = await subscriptionClient.GetRulesAsync();
            var defaultRule = rules.SingleOrDefault(rule => rule.Name == RuleDescription.DefaultRuleName);

            if (defaultRule != null)
            {
                await subscriptionClient.RemoveRuleAsync(defaultRule.Name);
            }
        }

        static async Task AddRuleIfNotExistsAsync(RuleDescription ruleDescription)
        {
            try
            {
                await subscriptionClient.AddRuleAsync(ruleDescription);
            }
            catch (ServiceBusException exception)
            {
                Console.WriteLine($"Could not add rule '{ruleDescription.Name}'. Exception: {exception.Message}.");
            }
        }

        static async Task CreateTopicIfNotExistsAsync(string topicName)
        {
            try
            {
                await managementClient.GetTopicAsync(topicName);
            }
            catch (MessagingEntityNotFoundException)
            {
                await managementClient.CreateTopicAsync(topicName);
            }
        }

        static async Task CreateSubscriptionIfNotExistsAsync(string topicName, string subscriptionName)
        {
            try
            {
                await managementClient.GetSubscriptionAsync(topicName, subscriptionName);
            }
            catch (MessagingEntityNotFoundException)
            {
                await managementClient.CreateSubscriptionAsync(new SubscriptionDescription(topicName, subscriptionName));
            }
        }
    }
}
