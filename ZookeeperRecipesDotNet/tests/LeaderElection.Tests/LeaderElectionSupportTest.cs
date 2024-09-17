﻿using FluentAssertions;
using org.apache.zookeeper;
using org.apache.zookeeper.recipes.leader;
using Serilog;
using Xunit;
using Xunit.Abstractions;

namespace LeaderElection.Tests;

public sealed class LeaderElectionSupportTest : IAsyncLifetime 
{
    private readonly ITestOutputHelper _testOutputHelper;
    private const int ConnectionTimeout = 4000;
    
    private readonly string _root = $"/{Guid.NewGuid()}";
    private ZooKeeper _zooKeeper;

    public LeaderElectionSupportTest(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    public async Task InitializeAsync()
    {
        _zooKeeper = await CreateZookeeperClient();
        await _zooKeeper.createAsync(_root, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public async Task DisposeAsync()
    {
        await ZKUtil.deleteRecursiveAsync(_zooKeeper, _root);
        await _zooKeeper.closeAsync();
    }

    [Fact]
    public async Task testNode() {
        var electionSupport = createLeaderElectionSupport();

        await electionSupport.start();
        await Task.Delay(3000);
        await electionSupport.stop();
    }

    private async Task CreateTestNodesTask(int testIterations, int millisecondsDelay)
    {
        Assert.True(await Task.WhenAll(Enumerable.Repeat(runElectionSupportTask(), testIterations))
            .WithTimeout(millisecondsDelay));
    }

    [Fact]
    public Task testNodes3() {
        return CreateTestNodesTask(3, 10 * 1000);
    }

    [Fact]
    public Task testNodes9() {
        return CreateTestNodesTask(9, 10 * 1000);
    }

    [Fact]
    public Task testNodes20() {
        return CreateTestNodesTask(20, 10 * 1000);
    }

    [Fact]
    public Task testNodes100() {
        return CreateTestNodesTask(100, 20 * 1000);
    }

    [Fact]
    public async Task testOfferShuffle() {
        const int testIterations = 10;

        var elections = Enumerable.Range(1, testIterations)
            .Select(i => runElectionSupportTask(Math.Min(i*1200, 10000)));
        Assert.True(await Task.WhenAll(elections).WithTimeout(60*1000));
    }

    [Fact]
    public async Task testGetLeaderHostName() {
        var electionSupport = createLeaderElectionSupport();

        await electionSupport.start();

        // Sketchy: We assume there will be a leader (probably us) in 3 seconds.
        await Task.Delay(3000);

        var leaderHostName = await electionSupport.getLeaderHostName();

        Assert.NotNull(leaderHostName);
        Assert.Equal("foohost", leaderHostName);

        await electionSupport.stop();
    }

    private class TestLeaderElectionAware(Func<ElectionEventType, Task> electionHandler) : LeaderElectionAware
    {
        public async Task onElectionEvent(ElectionEventType eventType)
        {
            await electionHandler(eventType);
        }
    }
        
    [Fact]
    public async Task testReadyOffer()
    {
        var events = new List<ElectionEventType>();
        var electedComplete = new TaskCompletionSource<bool>();
        
        var electionSupport1 = createLeaderElectionSupport();
        await electionSupport1.start();
        
        var electionSupport2 = createLeaderElectionSupport();
        var stoppedElectedNode = false;
        var listener = new TestLeaderElectionAware(async eventType =>
        {
            events.Add(eventType);
            if (!stoppedElectedNode
                && eventType == ElectionEventType.DETERMINE_COMPLETE) {
                stoppedElectedNode = true;
                try {
                    await electionSupport1.stop();
                } catch (Exception e) {
                    _testOutputHelper.WriteLine($"Unexpected exception, {e}");
                }
            }
            if (eventType == ElectionEventType.ELECTED_COMPLETE) {
                electedComplete.SetResult(true);
            }
        });
        
        electionSupport2.addListener(listener);
        await electionSupport2.start();

        _testOutputHelper.WriteLine(
            await Task.WhenAny(electedComplete.Task, Task.Delay(ConnectionTimeout / 3)) == electedComplete.Task
                ? "Re-election completed within the timeout."
                : "Re-election did not complete within the timeout.");
        
        var expectedEvents = new List<ElectionEventType>
        {
            ElectionEventType.START,
            ElectionEventType.OFFER_START,
            ElectionEventType.OFFER_COMPLETE,
            ElectionEventType.DETERMINE_START,
            ElectionEventType.DETERMINE_COMPLETE,
            ElectionEventType.READY_START,
            ElectionEventType.DETERMINE_START,
            ElectionEventType.DETERMINE_COMPLETE,
            ElectionEventType.ELECTED_START,
            ElectionEventType.ELECTED_COMPLETE,
            ElectionEventType.STOP_START,
            ElectionEventType.STOP_COMPLETE
        };
        
        await electionSupport2.stop();

        events.Should().BeEquivalentTo(expectedEvents, options => options.WithStrictOrdering());
    }

    private LeaderElectionSupport createLeaderElectionSupport()
    {
        var logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();
        return new LeaderElectionSupport(_zooKeeper, _root, "foohost", logger);
    }

    private async Task runElectionSupportTask(int sleepDuration = 3000)
    {
        var electionSupport = createLeaderElectionSupport();

        await electionSupport.start();
        await Task.Delay(sleepDuration);
        await electionSupport.stop();
    }
    
    private static async Task<ZooKeeper> CreateZookeeperClient()
    {
        var watcher = new ConnectedWatcher();
            
        string zookeeperConnectionString = "localhost:2181";
        var zk = new ZooKeeper(zookeeperConnectionString, ConnectionTimeout, watcher);

        if (!await watcher.WaitForConnection().WithTimeout(ConnectionTimeout))
        {
            Assert.Fail("Unable to connect to server");
        }
            
        return zk;
    }
}