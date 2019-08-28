using MQTTnet;
using MQTTnet.Client.Options;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Protocol;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestConsole1
{
    class Program
    {
        static void Main(string[] args)
        {
            Start();
            Console.ReadKey();

        }

        private static async Task Start()
        {
            var managedClient = new MqttFactory().CreateManagedMqttClient();

            var options = new ManagedMqttClientOptions
            {
                ClientOptions = new MqttClientOptions
                {
                    ClientId = "MQTTnetManagedClientTest",
                    Credentials = new MqttClientCredentials
                    {
                        Username = string.Empty,
                        Password = new byte[] { }
                    },

                    ChannelOptions = new MqttClientTcpOptions
                    {
                        Server = "localhost"
                    }
                },

                AutoReconnectDelay = TimeSpan.FromSeconds(1),
                //Storage = ms
            };

            managedClient.UseApplicationMessageReceivedHandler(HandleReceivedApplicationMessage);
            await managedClient.StartAsync(options);
            await managedClient.SubscribeAsync("home/garden/fountain", MqttQualityOfServiceLevel.AtLeastOnce);

        }

        private static async Task HandleReceivedApplicationMessage(MqttApplicationMessageReceivedEventArgs eventArgs)
        {
            string payload = Encoding.Default.GetString(eventArgs.ApplicationMessage.Payload);
            Console.WriteLine($"received from {eventArgs.ClientId}: {payload}");
            await Task.CompletedTask;
        }
    }
}
