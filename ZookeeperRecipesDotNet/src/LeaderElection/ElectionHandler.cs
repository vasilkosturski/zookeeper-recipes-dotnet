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
        private volatile bool _isLeader;

        public ElectionHandler(string zkConnectionString, string electionPath, ILogger logger)
        {
            _zkConnectionString = zkConnectionString;
            _electionPath = electionPath;
            _logger = logger;
        }
        
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.Information("Starting leader election process...");
            _leaderElection = await LeaderElection.Create(_zkConnectionString, _electionPath, this, _logger);
        }
        
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.Information("Stopping leader election process and closing resources...");
            if (_leaderElection != null)
            {
                await _leaderElection.Close();
            }
        }
        
        public Task OnElectionComplete(bool isLeader)
        {
            _isLeader = isLeader;
            _logger.Information(isLeader ? "This node has been elected as the leader." : "This node is a follower.");
            return Task.CompletedTask;
        }
        
        public bool IsLeader()
        {
            return _isLeader;
        }
    }
}
