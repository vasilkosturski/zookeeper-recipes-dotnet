namespace LeaderElection.Tests;

public static class Utils
{
    public static async Task<bool> WithTimeout(this Task task, int millisecondsTimeout)
    {
        Task delayTask = Task.Delay(millisecondsTimeout);
        await Task.WhenAny(task, delayTask).ConfigureAwait(false);
        return !delayTask.IsCompleted;
    }
}