using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace LeaderElection.Driver;

public static class Program
{
    static async Task Main()
    {
        // Create Docker network
        var network = new NetworkBuilder()
            .WithName("leader-election-network")
            .Build();
        await network.CreateAsync();

        // Create and start a ZooKeeper container
        var zookeeperContainer = new ContainerBuilder()
            .WithImage("zookeeper:3.6")
            .WithName("zookeeper-test")
            .WithPortBinding(2181, 2181)
            .WithNetwork(network)  // Add to network
            .Build();

        await zookeeperContainer.StartAsync();
        Console.WriteLine("ZooKeeper started!");

        // Start API instances
        var api1 = CreateApiInstance(5001, "node1", network);
        var api2 = CreateApiInstance(5002, "node2", network);
        var api3 = CreateApiInstance(5003, "node3", network);

        await Task.WhenAll(api1.StartAsync(), api2.StartAsync(), api3.StartAsync());
        Console.WriteLine("API instances started!");

        // Wait for leadership election to stabilize
        await Task.Delay(5000);

        // Initial leader status
        Console.WriteLine("Initial leader status:");
        await CheckLeaderStatus(5001);
        await CheckLeaderStatus(5002);
        await CheckLeaderStatus(5003);

        // Get the current leader
        var leaderPort = await FindLeader([5001, 5002, 5003]);
        Console.WriteLine($"The current leader is running on port {leaderPort}");

        // Stop the leader instance
        Console.WriteLine($"Stopping the leader API instance on port {leaderPort}...");
        await StopLeaderNode(leaderPort, api1, api2, api3);

        // Wait for leadership to change
        await Task.Delay(5000);

        // Check leader status for only running instances
        Console.WriteLine("Leader status after stopping the leader instance:");
        var remainingPorts = new[] { 5001, 5002, 5003 }.Where(p => p != leaderPort);
        foreach (var port in remainingPorts)
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

    static IContainer CreateApiInstance(int port, string name, INetwork network)
    {
        return new ContainerBuilder()
            .WithImage("leader-election-api-image") // Replace with your API image
            .WithName(name)
            .WithPortBinding(port, 8080) // Maps container port 8080 to your defined port
            .WithEnvironment("zkConnectionString", "zookeeper-test:2181")
            .WithNetwork(network)  // Add to the same network
            .Build();
    }

    static async Task CheckLeaderStatus(int port)
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

    static async Task<int> FindLeader(int[] ports)
    {
        foreach (var port in ports)
        {
            try
            {
                using var client = new HttpClient();
                var response = await client.GetStringAsync($"http://localhost:{port}/leader");
                if (response.Contains("\"isLeader\":true"))
                {
                    return port;
                }
            }
            catch (HttpRequestException)
            {
                // Skip if the instance is down
            }
        }
        throw new Exception("No leader found.");
    }

    static async Task StopLeaderNode(int leaderPort, IContainer api1, IContainer api2, IContainer api3)
    {
        if (leaderPort == 5001)
        {
            await api1.StopAsync();
        }
        else if (leaderPort == 5002)
        {
            await api2.StopAsync();
        }
        else if (leaderPort == 5003)
        {
            await api3.StopAsync();
        }
    }
}