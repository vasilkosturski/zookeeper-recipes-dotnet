using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace LeaderElection.Driver;

public static class Program
{
    private const int ApiInstanceOncePort = 5001;
    private const int ApiInstanceTwoPort = 5002;
    private const int ApiInstanceThreePort = 5003;
    private const string ZookeeperContainerName = "zookeeper-test";
    private const int ZookeeperPort = 2181;
    
    private static async Task Main()
    {
        // Create a network for the containers
        var dockerNetwork = new NetworkBuilder()
            .WithName("leader-election-network")
            .Build();
        await dockerNetwork.CreateAsync();

        // Start ZooKeeper container
        var zookeeperContainer = new ContainerBuilder()
            .WithImage("zookeeper:3.6")
            .WithName(ZookeeperContainerName)
            .WithPortBinding(ZookeeperPort, ZookeeperPort)
            .WithNetwork(dockerNetwork)
            .Build();

        await zookeeperContainer.StartAsync();
        Console.WriteLine("ZooKeeper started!");

        // Start API instances
        var api1 = CreateApiInstance(ApiInstanceOncePort, "instance1", dockerNetwork);
        var api2 = CreateApiInstance(ApiInstanceTwoPort, "instance2", dockerNetwork);
        var api3 = CreateApiInstance(ApiInstanceThreePort, "instance3", dockerNetwork);

        await Task.WhenAll(api1.StartAsync(), api2.StartAsync(), api3.StartAsync());
        Console.WriteLine("API instances started!");

        // Check leader status for all instances
        Console.WriteLine("Initial leader status:");
        await CheckLeaderStatus(ApiInstanceOncePort);
        await CheckLeaderStatus(ApiInstanceTwoPort);
        await CheckLeaderStatus(ApiInstanceThreePort);

        // Find the current leader
        var leaderPort = await FindLeader([ApiInstanceOncePort, ApiInstanceTwoPort, ApiInstanceThreePort]);
        Console.WriteLine($"The current leader is running on port {leaderPort}");

        // Stop the leader instance
        Console.WriteLine($"Stopping the leader API instance on port {leaderPort}...");
        await StopLeaderNode(leaderPort, api1, api2, api3);

        // Wait for leadership to change
        await Task.Delay(1000);

        // Check leader status for only running instances
        Console.WriteLine("Leader status after stopping the leader instance:");
        var remainingInstancesPorts = new[] { ApiInstanceOncePort, ApiInstanceTwoPort, ApiInstanceThreePort }
            .Where(p => p != leaderPort);
        foreach (var port in remainingInstancesPorts)
        {
            await CheckLeaderStatus(port);
        }

        // Cleanup
        await api1.StopAsync();
        await api2.StopAsync();
        await api3.StopAsync();
        await zookeeperContainer.StopAsync();

        Console.WriteLine("Test completed.");
    }

    private static IContainer CreateApiInstance(int port, string name, INetwork network)
    {
        return new ContainerBuilder()
            .WithImage("leader-election-api-image")
            .WithName(name)
            .WithPortBinding(port, 8080)
            .WithEnvironment("zkConnectionString", $"{ZookeeperContainerName}:{ZookeeperPort}")
            .WithNetwork(network)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8080))
            .Build();
    }

    private static async Task CheckLeaderStatus(int port)
    {
        try
        {
            using var client = new HttpClient();
            var response = await client.GetStringAsync($"http://localhost:{port}/leader");
            Console.WriteLine($"Leader status from port {port}: {response}");
        }
        catch (HttpRequestException)
        {
            Console.WriteLine($"Could not connect to API on port {port}. It might be stopped.");
        }
    }

    private static async Task<int> FindLeader(int[] ports)
    {
        foreach (var port in ports)
        {
            using var client = new HttpClient();
            var response = await client.GetStringAsync($"http://localhost:{port}/leader");
            if (response.Contains("\"isLeader\":true"))
            {
                return port;
            }
        }
        throw new Exception("No leader found.");
    }

    private static async Task StopLeaderNode(int leaderPort, IContainer api1, IContainer api2, IContainer api3)
    {
        if (leaderPort == ApiInstanceOncePort)
        {
            await api1.StopAsync();
        }
        else if (leaderPort == ApiInstanceTwoPort)
        {
            await api2.StopAsync();
        }
        else if (leaderPort == ApiInstanceThreePort)
        {
            await api3.StopAsync();
        }
    }
}
