using org.apache.zookeeper;
using Nito.AsyncEx;

namespace LeaderElection;

public class LeaderElection : Watcher
{
    private readonly AsyncLock _mutex = new();
    private readonly string _zkConnectionString;
    private readonly string _electionPath;
    private readonly IElectionHandler _electionHandler;
    private ZooKeeper _zooKeeper;
    private string _znodePath;

    public LeaderElection(string zkConnectionString, string electionPath, IElectionHandler electionHandler)
    {
        _zkConnectionString = zkConnectionString;
        _electionPath = electionPath;
        _electionHandler = electionHandler;
        InitializeZooKeeper();
    }

    private void InitializeZooKeeper()
    {
        const int sessionTimeoutMillis = 30000;
        _zooKeeper = new ZooKeeper(_zkConnectionString, sessionTimeoutMillis, this);
    }

    public async Task RegisterForElection()
    {
        await EnsureElectionPathExistsAsync();

        _znodePath = await _zooKeeper.createAsync($"{_electionPath}/n_", new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        await CheckLeadership();
    }

    private async Task EnsureElectionPathExistsAsync()
    {
        if (await _zooKeeper.existsAsync(_electionPath) == null)
        {
            try
            {
                await _zooKeeper.createAsync(_electionPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch (KeeperException.NodeExistsException)
            {
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

            if (currentNodeName == children[0])
            {
                await _electionHandler.OnElectionComplete(true);
            }
            else
            {
                var index = children.IndexOf(currentNodeName);
                if (index > 0)
                {
                    var previousNode = $"{_electionPath}/{children[index - 1]}";
                    await _zooKeeper.existsAsync(previousNode, true);
                }
            }
        }
    }

    public override async Task process(WatchedEvent @event)
    {
        if (@event.get_Type() == Event.EventType.NodeDeleted)
        {
            await CheckLeadership();
        }
    }

    public async Task Close()
    {
        if (_zooKeeper != null)
        {
            await _zooKeeper.closeAsync();
            _zooKeeper = null;
        }
    }
}