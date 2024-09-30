namespace LeaderElection;

public interface IElectionHandler
{
    Task OnElectionComplete(bool isLeader);
    bool IsLeader();
}

public class ElectionHandler : IElectionHandler, IHostedService
{
    private readonly LeaderElection _leaderElection;
    private readonly ILogger<ElectionHandler> _logger;
    private bool _isLeader;

    public ElectionHandler(LeaderElection leaderElection, ILogger<ElectionHandler> logger)
    {
        _leaderElection = leaderElection;
        _logger = logger;
    }
    
    public Task OnElectionComplete(bool isLeader)
    {
        _isLeader = isLeader;

        if (isLeader)
        {
            _logger.LogInformation("This node has been elected as the leader.");
        }
        else
        {
            _logger.LogInformation("This node is a follower.");
        }

        return Task.CompletedTask;
    }

    public bool IsLeader()
    {
        return _isLeader;
    }
    
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting the leader election process.");
        await _leaderElection.RegisterForElection(); // Trigger leader election process at startup
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping the leader election process and closing resources.");
        await _leaderElection.Close(); // Ensure ZooKeeper is properly closed on shutdown
    }
}