using org.apache.zookeeper;

namespace LeaderElection.Tests;

public class ConnectedWatcher : Watcher
{
    private readonly TaskCompletionSource<bool> _connectedTaskCompletionSource = new();

    public Task WaitForConnection()
    {
        return _connectedTaskCompletionSource.Task;
    }

    public override Task process(WatchedEvent @event)
    {
        if (@event.getState() == Event.KeeperState.SyncConnected)
        {
            _connectedTaskCompletionSource.TrySetResult(true);
        }
        else if (@event.getState() == Event.KeeperState.Disconnected || @event.getState() == Event.KeeperState.Expired)
        {
            _connectedTaskCompletionSource.TrySetException(new Exception("ZooKeeper connection failed."));
        }

        return Task.CompletedTask;
    }
}