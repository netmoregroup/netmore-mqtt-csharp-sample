using System;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Extensions.ManagedClient;
using System.Security.Cryptography.X509Certificates;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.IO;

namespace mqtt_sample
{
    public class SendML {
        public string n { get; set; }
        public Int64 t { get; set; }
        public JsonElement v { get; set; }
        public string u { get; set; }
    }
    class Program
    {
        public static void OnConnected(MqttClientConnectedEventArgs obj)
        {
            Console.WriteLine("Successfully connected.");
        }

        public static void OnConnectingFailed(ManagedProcessFailedEventArgs obj)
        {
            Console.WriteLine("Couldn't connect to broker.");
        }

        public static void OnDisconnected(MqttClientDisconnectedEventArgs obj)
        {
            Console.WriteLine("Successfully disconnected.");
        }
        static void Main(string[] args)
        {
            if( args.Length == 0 ) {
                Console.WriteLine("You have to start with a valid host name as argument.");
                return;
            }
            var host = args[0];
            var port = 8883;

            Console.WriteLine("Connecting to host {0}", host);
            var clientCert = new X509Certificate2(@"./certs/client.pfx");            
            var parts = clientCert.Subject.Split(",");
            var cn = (parts[0].Split("="))[1];
            Console.WriteLine("Common name: {0}", cn);
            X509Certificate2 caCrt = new X509Certificate2(File.ReadAllBytes(@"./certs/ca.crt"));

            var options = new ManagedMqttClientOptionsBuilder()
                .WithClientOptions(new MqttClientOptionsBuilder()
                    .WithClientId(Guid.NewGuid().ToString())
                    .WithTcpServer(host, port)
                    .WithTls(new MqttClientOptionsBuilderTlsParameters()
                    {
                        UseTls = true,
                        //AllowUntrustedCertificates = true,
                        //IgnoreCertificateChainErrors = true,
                        //IgnoreCertificateRevocationErrors = true,

                        Certificates = new List<X509Certificate>()
                        {
                            clientCert, caCrt
                        },
                        CertificateValidationHandler = (certContext) => {
                            X509Chain chain = new X509Chain();
                            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                            chain.ChainPolicy.RevocationFlag = X509RevocationFlag.ExcludeRoot;
                            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;
                            chain.ChainPolicy.VerificationTime = DateTime.Now;
                            chain.ChainPolicy.UrlRetrievalTimeout = new TimeSpan(0, 0, 0);
                            chain.ChainPolicy.CustomTrustStore.Add(caCrt);
                            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;

                            // convert provided X509Certificate to X509Certificate2
                            var x5092 = new X509Certificate2(certContext.Certificate);

                            return chain.Build(x5092);
                        }
                    })
                    .Build())
                .Build();


            IManagedMqttClient _mqttClient = new MqttFactory().CreateManagedMqttClient();
            _mqttClient.ConnectedHandler = new MqttClientConnectedHandlerDelegate(OnConnected);
            _mqttClient.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(OnDisconnected);
            _mqttClient.ConnectingFailedHandler = new ConnectingFailedHandlerDelegate(OnConnectingFailed);
            
            
            // Starts a connection with the Broker
            _mqttClient.StartAsync(options).GetAwaiter().GetResult();
            _mqttClient.SubscribeAsync(String.Format("client/{0}/#", cn));
            _mqttClient.UseApplicationMessageReceivedHandler(e =>
                {
                    var x = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                    Console.WriteLine("### RECEIVED APPLICATION MESSAGE ###");
                    Console.WriteLine($"+ Topic   = {e.ApplicationMessage.Topic}");                    
                    SendML[] messages = JsonSerializer.Deserialize<SendML[]>(x);
                    foreach( var msg in messages) {
                        Console.WriteLine("###################################");
                        Console.WriteLine($"+ n   = {msg.n}");
                        Console.WriteLine($"+ u   = {msg.u}");
                        if (msg.n == "raw" && msg.u == "b64")
                        {
                            byte[] data = Convert.FromBase64String(msg.v.ToString());
                            string decodedString = Encoding.UTF8.GetString(data);
                            Console.WriteLine($"+ Raw data: {msg.v}");
                            Console.WriteLine($"+ Raw data: {decodedString}");
                        }
                        else if (msg.n == "raw" && msg.u == "json")
                        {
                            Console.WriteLine($"+ Raw data: {msg.v}");
                        }
                        else
                        {
                            Console.WriteLine($"+ Payload = {x}");
                            switch (msg.v.ValueKind)
                            {
                                case JsonValueKind.Number:
                                    Console.WriteLine(msg.v);
                                    break;
                            }
                        }
                    }
            });
            while (true)
            {
                //_mqttClient.PublishAsync("dev.to/topic/json", "{ \"xxx\": \"xx\" }");
                Task.Delay(1000)
                    .GetAwaiter()
                    .GetResult();
            }
        }
    }
}
