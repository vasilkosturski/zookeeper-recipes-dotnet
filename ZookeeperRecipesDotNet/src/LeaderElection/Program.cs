using Serilog;
using LeaderElection;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog(logger);

var zkConnectionString = Environment.GetEnvironmentVariable("zkConnectionString") ?? "localhost:2181";
var electionPath = Environment.GetEnvironmentVariable("electionPath") ?? "/leader-election";

builder.Services.AddSingleton<IElectionHandler, ElectionHandler>(_ =>
    new ElectionHandler(zkConnectionString, electionPath, logger));

builder.Services.AddHostedService(sp =>
    (ElectionHandler)sp.GetRequiredService<IElectionHandler>());

var app = builder.Build();

app.MapGet("/leader", ([FromServices] IElectionHandler electionHandler) =>
{
    var isLeader = electionHandler.IsLeader();
    return Results.Json(new { isLeader });
});

app.Run();