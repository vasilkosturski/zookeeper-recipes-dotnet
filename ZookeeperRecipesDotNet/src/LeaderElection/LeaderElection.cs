using org.apache.zookeeper;
using System.Threading.Channels;

namespace LeaderElection;

public class LeaderElection : Watcher
{
    private readonly string _zkConnectionString;
    private readonly string _electionPath;
    private readonly IElectionHandler _electionHandler;
    private ZooKeeper _zooKeeper;
    private string _znodePath;

    // Channel to queue leadership tasks
    private readonly Channel<Func<Task>> _leadershipTaskChannel = Channel.CreateUnbounded<Func<Task>>();

    public LeaderElection(string zkConnectionString, string electionPath, IElectionHandler electionHandler)
    {
        _zkConnectionString = zkConnectionString;
        _electionPath = electionPath;
        _electionHandler = electionHandler;
        InitializeZooKeeper();

        // Start the task processor to process leadership checks from the channel
        Task.Run(ProcessLeadershipTasksAsync);
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

        // Enqueue the leadership check task in the channel
        await _leadershipTaskChannel.Writer.WriteAsync(CheckLeadershipAsync);
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
                // Path already exists, nothing to do here
            }
        }
    }

    // Channel processor that serializes leadership tasks
    private async Task ProcessLeadershipTasksAsync()
    {
        await foreach (var leadershipTask in _leadershipTaskChannel.Reader.ReadAllAsync())
        {
            await leadershipTask();
        }
    }

    // The leadership check logic enqueued into the channel
    private async Task CheckLeadershipAsync()
    {
        var children = (await _zooKeeper.getChildrenAsync(_electionPath)).Children;
        children.Sort();

        var currentNodeName = _znodePath.Substring(_znodePath.LastIndexOf('/') + 1);

        // If the current node is the first in the list, it's the leader
        var isLeader = currentNodeName == children[0]; 
        if (isLeader)
        {
            await _electionHandler.OnElectionComplete(true);
        }
        else
        {
            var index = children.IndexOf(currentNodeName);
            if (index > 0)
            {
                var previousNode = $"{_electionPath}/{children[index - 1]}";
                    
                var stat = await _zooKeeper.existsAsync(previousNode, true);

                // The node has gone missing between the call to getChildren() and exists().
                // We need to try and become the leader.
                if (stat == null)
                {
                    await CheckLeadershipAsync();
                }
            }
        }
    }

    public override async Task process(WatchedEvent @event)
    {
        if (@event.get_Type() == Event.EventType.NodeDeleted)
        {
            // Enqueue a leadership check when a node is deleted
            await _leadershipTaskChannel.Writer.WriteAsync(CheckLeadershipAsync);
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
