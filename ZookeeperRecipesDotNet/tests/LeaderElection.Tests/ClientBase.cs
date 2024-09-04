using System.Collections.Concurrent;
using org.apache.zookeeper;
using Xunit;

namespace LeaderElection.Tests 
{
    [Collection("Setup")]
    public abstract class ClientBase : IAsyncLifetime
    {
        private const int ConnectionTimeout = 4000;
        private string _currentRoot;
        private ZooKeeper _rootZk;

        private const string HostPort = "127.0.0.1,localhost";

        private readonly ConcurrentBag<ZooKeeper> _allClients = [];
        
        protected async Task<ZooKeeper> CreateClient(string chroot = null)
        {
            var watcher = new ConnectedWatcher();
            var zk = new ZooKeeper(HostPort + _currentRoot + chroot, ConnectionTimeout, watcher);

            if (!await watcher.WaitForConnection().WithTimeout(ConnectionTimeout))
            {
                Assert.Fail("Unable to connect to server");
            }
            
            _allClients.Add(zk);
            
            return zk;
        }

        public virtual async Task InitializeAsync()
        {
            _rootZk = await CreateClient();
            _currentRoot = await _rootZk.createAsync("/", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        }
        
        public virtual async Task DisposeAsync()
        {
            await ZKUtil.deleteRecursiveAsync(_rootZk, _currentRoot);
            await Task.WhenAll(_allClients.Select(c => c.closeAsync()));
        }
    }
    
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
}