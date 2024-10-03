﻿using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace LeaderElection.Driver;

public static class Program
{
    static async Task Main(string[] args)
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
        var api1 = CreateApiInstance(5001, "leader1", network);
        var api2 = CreateApiInstance(5002, "leader2", network);
        var api3 = CreateApiInstance(5003, "leader3", network);

        await Task.WhenAll(api1.StartAsync(), api2.StartAsync(), api3.StartAsync());
        Console.WriteLine("API instances started!");

        // Wait for leadership election to stabilize
        await Task.Delay(5000);

        // Test leader status
        await CheckLeaderStatus(5001);
        await CheckLeaderStatus(5002);
        await CheckLeaderStatus(5003);

        // Simulate killing one of the API instances
        Console.WriteLine("Stopping API instance on port 5001...");
        await api1.StopAsync();

        // Wait for leadership to change
        await Task.Delay(2000);

        // Check leader status again
        await CheckLeaderStatus(5002);
        await CheckLeaderStatus(5003);

        // Cleanup
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
            .WithPortBinding(port, 8080) // Maps container port 80 to your defined port
            .WithEnvironment("zkConnectionString", "zookeeper-test:2181")
            .WithNetwork(network)  // Add to the same network
            .Build();
    }

    static async Task CheckLeaderStatus(int port)
    {
        using var client = new HttpClient();
        var response = await client.GetStringAsync($"http://localhost:{port}/leader");
        Console.WriteLine($"Leader status from port {port}: {response}");
    }
}
