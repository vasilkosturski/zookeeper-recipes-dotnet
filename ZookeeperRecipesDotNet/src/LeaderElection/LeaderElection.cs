using org.apache.zookeeper;
using Nito.AsyncEx;
using ILogger = Serilog.ILogger;

namespace LeaderElection;

public interface IElectionHandler
{
    public Task OnElectionComplete(bool isLeader);
}

public class LeaderElection : Watcher
{
    private readonly AsyncLock _mutex = new();
    
    private readonly IElectionHandler _electionHandler;
    private readonly string _zkConnectionString;
    private readonly string _electionPath;
    private readonly ILogger _logger;
    
    private ZooKeeper _zooKeeper;
    private string _znodePath;
    private bool _isLeader;

    public LeaderElection(string zkConnectionString, string electionPath, IElectionHandler electionHandler, ILogger logger)
    {
        _zkConnectionString = zkConnectionString;
        _electionPath = electionPath;
        _electionHandler = electionHandler;
        _logger = logger;
        InitializeZooKeeper();
    }
    
    public async Task<bool> IsLeaderAsync()
    {
        using (await _mutex.LockAsync())
        {
            return _isLeader;
        }
    }

    public async Task RegisterForElection()
    {
        await EnsureElectionPathExistsAsync();
        
        _znodePath = await _zooKeeper.createAsync($"{_electionPath}/n_", [],
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        
        await CheckLeadership();
    }
    
    private void InitializeZooKeeper()
    {
        const int sessionTimeoutMillis = 30000;
        _zooKeeper = new ZooKeeper(_zkConnectionString, sessionTimeoutMillis, this);
    }

    private async Task EnsureElectionPathExistsAsync()
    {
        if (await _zooKeeper.existsAsync(_electionPath) == null)
        {
            try
            {
                await _zooKeeper.createAsync(_electionPath, [],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch (KeeperException.NodeExistsException)
            {
                // Race condition: another instance created the path before this one
            }
        }
    }

    private async Task CheckLeadership()
    {
        using (await _mutex.LockAsync())
        {
            var children = (await _zooKeeper.getChildrenAsync(_electionPath)).Children;
            children.Sort();

            var currentNodeName = _znodePath.Substring(_znodePath.LastIndexOf('/') + 1);

            var currentNodeIsLeader = currentNodeName == children[0];
            if (currentNodeIsLeader)
            {
                if (!_isLeader)
                {
                    _isLeader = true;
                    try
                    {
                        if (_electionHandler != null)
                        {
                            await _electionHandler.OnElectionComplete(true);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ex, "Error handling election completion");
                    }
                }
            }
            else
            {
                _isLeader = false;
                var index = children.IndexOf(currentNodeName);
                if (index > 0)
                {
                    var previousNode = $"{_electionPath}/{children[index - 1]}";
                    await _zooKeeper.existsAsync(previousNode, true);
                }
            }
        }
    }

    public async Task CloseAsync()
    {
        try
        {
            if (_zooKeeper != null)
            {
                await _zooKeeper.closeAsync();
                _zooKeeper = null;
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error while closing ZooKeeper session");
        }
    }

    public override async Task process(WatchedEvent @event)
    {
        if (@event.get_Type() == Event.EventType.NodeDeleted)
        {
            // Re-check leadership when a node is deleted
            await CheckLeadership();
        }
    }
}
