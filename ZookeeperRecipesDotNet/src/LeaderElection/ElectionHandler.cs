using ILogger = Serilog.ILogger;

namespace LeaderElection
{
    public interface IElectionHandler
    {
        Task OnElectionComplete(bool isLeader);
        bool IsLeader();
    }

    public class ElectionHandler : IElectionHandler, IHostedService
    {
        private readonly string _zkConnectionString;
        private readonly string _electionPath;
        private readonly ILogger _logger;
        private LeaderElection _leaderElection;
        private bool _isLeader;

        public ElectionHandler(string zkConnectionString, string electionPath, ILogger logger)
        {
            _zkConnectionString = zkConnectionString;
            _electionPath = electionPath;
            _logger = logger;
        }

        // This method is invoked by LeaderElection when the node becomes a leader or follower
        public Task OnElectionComplete(bool isLeader)
        {
            _isLeader = isLeader;

            if (isLeader)
            {
                _logger.Information("This node has been elected as the leader.");
            }
            else
            {
                _logger.Information("This node is a follower.");
            }

            return Task.CompletedTask;
        }

        // Return whether this node is the leader
        public bool IsLeader()
        {
            return _isLeader;
        }

        // IHostedService method - starts the leader election process
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.Information("Starting leader election process...");

            // Use the async factory method to create the LeaderElection instance
            _leaderElection = await LeaderElection.CreateAsync(_zkConnectionString, _electionPath, this, _logger);

            // Now that leader election is registered, the process is running
        }

        // IHostedService method - stops the leader election process
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.Information("Stopping leader election process and closing resources...");
            if (_leaderElection != null)
            {
                await _leaderElection.Close(); // Ensure ZooKeeper is properly closed on shutdown
            }
        }
    }
}
