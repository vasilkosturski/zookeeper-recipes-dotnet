﻿using org.apache.zookeeper;
using System.Threading.Channels;
using ILogger = Serilog.ILogger;

namespace LeaderElection;

public class LeaderElection : Watcher
{
    private readonly string _zkConnectionString;
    private readonly string _electionPath;
    private readonly IElectionHandler _electionHandler;
    private readonly ILogger _logger;
    private ZooKeeper _zooKeeper;
    private string _znodePath;

    // Channel to queue leadership tasks
    private readonly Channel<Func<Task>> _leadershipTaskChannel = Channel.CreateUnbounded<Func<Task>>();

    public LeaderElection(string zkConnectionString, string electionPath, IElectionHandler electionHandler, ILogger logger)
    {
        _zkConnectionString = zkConnectionString;
        _electionPath = electionPath;
        _electionHandler = electionHandler;
        _logger = logger;
        InitializeZooKeeper();

        // Start the task processor to process leadership checks from the channel
        Task.Run(ProcessLeadershipTasksAsync);
    }

    private void InitializeZooKeeper()
    {
        const int sessionTimeoutMillis = 30000;
        _logger.Information("Initializing ZooKeeper connection...");
        _zooKeeper = new ZooKeeper(_zkConnectionString, sessionTimeoutMillis, this);
    }

    public async Task RegisterForElection()
    {
        await EnsureElectionPathExistsAsync();

        _znodePath = await _zooKeeper.createAsync($"{_electionPath}/n_", new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        _logger.Information("Node created: {ZnodePath}. Enqueueing leadership check.", _znodePath);

        // Enqueue the leadership check task in the channel
        await _leadershipTaskChannel.Writer.WriteAsync(CheckLeadershipAsync);
    }

    private async Task EnsureElectionPathExistsAsync()
    {
        _logger.Information("Ensuring election path exists...");
        if (await _zooKeeper.existsAsync(_electionPath) == null)
        {
            try
            {
                await _zooKeeper.createAsync(_electionPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                _logger.Information("Election path created: {ElectionPath}", _electionPath);
            }
            catch (KeeperException.NodeExistsException)
            {
                _logger.Information("Election path already exists: {ElectionPath}", _electionPath);
            }
        }
    }

    // Channel processor that serializes leadership tasks
    private async Task ProcessLeadershipTasksAsync()
    {
        _logger.Information("Starting leadership task processing...");
        await foreach (var leadershipTask in _leadershipTaskChannel.Reader.ReadAllAsync())
        {
            _logger.Information("Processing leadership task...");
            await leadershipTask();
        }
        _logger.Information("Leadership task processing stopped.");
    }

    // The leadership check logic enqueued into the channel
    private async Task CheckLeadershipAsync()
    {
        try
        {
            _logger.Information("Checking leadership status...");

            var children = (await _zooKeeper.getChildrenAsync(_electionPath)).Children;
            children.Sort();

            var currentNodeName = _znodePath.Substring(_znodePath.LastIndexOf('/') + 1);

            // If the current node is the first in the list, it's the leader
            var isLeader = currentNodeName == children[0];
            if (isLeader)
            {
                _logger.Information("Node {Node} is the leader.", currentNodeName);
                await _electionHandler.OnElectionComplete(true);
            }
            else
            {
                _logger.Information("Node {Node} is not the leader.", currentNodeName);
                var index = children.IndexOf(currentNodeName);
                if (index > 0)
                {
                    var previousNode = $"{_electionPath}/{children[index - 1]}";
                    _logger.Information("Watching previous node: {PreviousNode}", previousNode);

                    var stat = await _zooKeeper.existsAsync(previousNode, true);

                    // The node has gone missing between the call to getChildren() and exists().
                    if (stat == null)
                    {
                        _logger.Information("Previous node {PreviousNode} no longer exists. Rechecking leadership.", previousNode);
                        await CheckLeadershipAsync();
                    }
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
            // Enqueue a leadership check when a node is deleted
            await _leadershipTaskChannel.Writer.WriteAsync(CheckLeadershipAsync);
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
        _leadershipTaskChannel.Writer.Complete();
    }
}
