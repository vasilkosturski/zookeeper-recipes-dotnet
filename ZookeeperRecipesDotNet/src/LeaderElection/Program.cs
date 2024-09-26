using Serilog;
using LeaderElection;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog(logger);

builder.Services.AddSingleton<IElectionHandler, ElectionHandler>();
builder.Services.AddSingleton<LeaderElection.LeaderElection>();

var app = builder.Build();

var leaderElection = app.Services.GetRequiredService<LeaderElection.LeaderElection>();
await leaderElection.RegisterForElection();

app.MapGet("/leader", (IElectionHandler electionHandler) =>
{
    var isLeader = electionHandler.IsLeader();
    return Results.Json(new { isLeader });
});

app.Run();