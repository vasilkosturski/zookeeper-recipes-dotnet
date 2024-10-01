using org.apache.zookeeper;
using System.Threading.Channels;
using ILogger = Serilog.ILogger;

namespace LeaderElection;

public class LeaderElection : Watcher
{
    private readonly string _electionPath;
    private readonly IElectionHandler _electionHandler;
    private readonly ILogger _logger;
    private ZooKeeper _zooKeeper;
    private string _znodePath;
    
    private readonly Channel<Func<Task>> _checkLeadershipChannel = Channel.CreateUnbounded<Func<Task>>();
    
    private LeaderElection(string zkConnectionString, string electionPath, IElectionHandler electionHandler, ILogger logger)
    {
        _electionPath = electionPath;
        _electionHandler = electionHandler;
        _logger = logger;

        const int sessionTimeoutMillis = 30000;
        _logger.Information("Initializing ZooKeeper connection...");
        _zooKeeper = new ZooKeeper(zkConnectionString, sessionTimeoutMillis, this);
    }
    
    public static async Task<LeaderElection> Create(
        string zkConnectionString, string electionPath, IElectionHandler electionHandler, ILogger logger)
    {
        var instance = new LeaderElection(zkConnectionString, electionPath, electionHandler, logger);
        
        _ = Task.Run(instance.ProcessLeadershipTasks);
        await instance.RegisterForElection();
        
        return instance;
    }
    
    private async Task RegisterForElection()
    {
        await EnsureElectionPathExists();

        _znodePath = await _zooKeeper.createAsync($"{_electionPath}/n_", [],
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        _logger.Information("Node created: {ZnodePath}. Enqueueing leadership check.", _znodePath);
        
        await _checkLeadershipChannel.Writer.WriteAsync(CheckLeadership);
    }

    private async Task EnsureElectionPathExists()
    {
        _logger.Information("Ensuring election path exists...");
        if (await _zooKeeper.existsAsync(_electionPath) == null)
        {
            try
            {
                await _zooKeeper.createAsync(_electionPath, [], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                _logger.Information("Election path created: {ElectionPath}", _electionPath);
            }
            catch (KeeperException.NodeExistsException)
            {
                _logger.Information("Election path already exists: {ElectionPath}", _electionPath);
            }
        }
    }
    
    private async Task ProcessLeadershipTasks()
    {
        try
        {
            _logger.Information("Starting leadership task processing...");
            await foreach (var leadershipTask in _checkLeadershipChannel.Reader.ReadAllAsync())
            {
                _logger.Information("Processing leadership task...");
                await leadershipTask();
            }
            _logger.Information("Leadership task processing stopped.");
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "An error occurred while processing leadership tasks.");
        }
    }
    
    private async Task CheckLeadership()
    {
        try
        {
            _logger.Information("Checking leadership status...");

            var children = (await _zooKeeper.getChildrenAsync(_electionPath)).Children;
            children.Sort();

            var currentNodeName = _znodePath[(_znodePath.LastIndexOf('/') + 1)..];
            
            var isLeader = currentNodeName == children[0];
            if (isLeader)
            {
                _logger.Information("Node {Node} is the leader.", currentNodeName);
                await _electionHandler.OnElectionComplete(true);
                return;
            }
            
            _logger.Information("Node {Node} is not the leader.", currentNodeName);
            var index = children.IndexOf(currentNodeName);
            if (index != -1)
            {
                var previousNode = $"{_electionPath}/{children[index - 1]}";
                _logger.Information("Watching previous node: {PreviousNode}", previousNode);

                var prevNodeStat = await _zooKeeper.existsAsync(previousNode, true);

                // The node has gone missing between the call to getChildren() and exists().
                if (prevNodeStat == null)
                {
                    _logger.Information("Previous node {PreviousNode} no longer exists. Rechecking leadership.", previousNode);
                    await CheckLeadership();
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error occurred during leadership check.");
        }
    }

    public override async Task process(WatchedEvent @event)
    {
        if (@event.get_Type() == Event.EventType.NodeDeleted)
        {
            _logger.Information("Node deleted event detected. Enqueueing leadership check.");
            await _checkLeadershipChannel.Writer.WriteAsync(CheckLeadership);
        }
    }

    public async Task Close()
    {
        if (_zooKeeper != null)
        {
            _logger.Information("Closing ZooKeeper connection...");
            await _zooKeeper.closeAsync();
            _zooKeeper = null;
        }

        _logger.Information("Closing leadership task channel writer.");
        _checkLeadershipChannel.Writer.Complete();
    }
}