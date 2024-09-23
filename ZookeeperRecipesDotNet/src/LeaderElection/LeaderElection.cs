﻿using org.apache.zookeeper;
using Serilog;
using Nito.AsyncEx;

namespace LeaderElection;

public interface IElectionHandler
{
    public Task OnElectionComplete(bool isLeader);
}

public class LeaderElection : Watcher
{
    private readonly string _zkConnectionString;
    private readonly string _electionPath;
    private readonly ILogger _logger;
    private ZooKeeper _zooKeeper;
    private string _znodePath;
    private bool _isLeader;
    private readonly AsyncLock _mutex = new();

    public IElectionHandler ElectionHandler { get; set; }

    public LeaderElection(string zkConnectionString, string electionPath, ILogger logger)
    {
        _zkConnectionString = zkConnectionString;
        _electionPath = electionPath;
        _logger = logger;
        InitializeZooKeeper();
    }

    public async Task RegisterForElection()
    {
        await EnsureElectionPathExistsAsync();
        
        _znodePath = await _zooKeeper.createAsync($"{_electionPath}/n_", new byte[0],  // Corrected to new byte[0]
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

            var currentNodeName = _znodePath.Substring(_znodePath.LastIndexOf("/") + 1);

            if (currentNodeName == children[0])
            {
                if (!_isLeader)
                {
                    _isLeader = true;
                    try
                    {
                        if (ElectionHandler != null)
                        {
                            await ElectionHandler.OnElectionComplete(true);
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
                // Watch the node just before this one in the sorted list
                int index = children.IndexOf(currentNodeName);
                if (index > 0)
                {
                    string previousNode = $"{_electionPath}/{children[index - 1]}";
                    await _zooKeeper.existsAsync(previousNode, true);  // Set a watch on the previous node
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