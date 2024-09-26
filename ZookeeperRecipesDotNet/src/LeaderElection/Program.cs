using Serilog;
using LeaderElection;

var builder = WebApplication.CreateBuilder(args);

// Set up logging
var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();
builder.Logging.ClearProviders();
builder.Logging.AddSerilog(logger);

// Add required services
builder.Services.AddSingleton<IElectionHandler, ElectionHandler>();
builder.Services.AddSingleton<LeaderElection.LeaderElection>();

var app = builder.Build();

// Start leader election when the application starts
var leaderElection = app.Services.GetRequiredService<LeaderElection.LeaderElection>();
await leaderElection.RegisterForElection(); // Start the election process

// Define the API to check if this instance is the leader
app.MapGet("/leader", async (LeaderElection.LeaderElection leaderElection) =>
{
    var isLeader = await leaderElection.IsLeaderAsync();
    return Results.Json(new { isLeader });
});

app.Run();