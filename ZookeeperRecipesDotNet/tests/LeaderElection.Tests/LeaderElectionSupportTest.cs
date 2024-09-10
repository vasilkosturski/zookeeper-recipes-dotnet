using org.apache.zookeeper;
using org.apache.zookeeper.recipes.leader;
using Serilog;
using Xunit;

namespace LeaderElection.Tests;

public sealed class LeaderElectionSupportTest : IAsyncLifetime 
{
    private const int ConnectionTimeout = 4000;
    private static int _globalCounter;
    
    private readonly string _root = "/" + Interlocked.Increment(ref _globalCounter);
    private ZooKeeper _zooKeeper;

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