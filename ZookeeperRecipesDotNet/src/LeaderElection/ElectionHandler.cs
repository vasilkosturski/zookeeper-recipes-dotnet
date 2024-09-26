namespace LeaderElection;

public interface IElectionHandler
{
    Task OnElectionComplete(bool isLeader);
    bool IsLeader();
}

public class ElectionHandler : IElectionHandler
{
    private readonly ILogger<ElectionHandler> _logger;
    private bool _isLeader;

    public ElectionHandler(ILogger<ElectionHandler> logger)
    {
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
}