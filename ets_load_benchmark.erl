-module(ets_load_benchmark).

-export([start/0]).

-define(CollectTime, timer:seconds(5)).
-define(Table1, ets_load_benchmark_public).
-define(Table2, ets_load_benchmark_public_2).
-define(SchedulerId(), erlang:system_info(scheduler_id)).

start() ->
    ets:new(?Table1, [public, named_table]),
    ets:new(?Table2, [public, named_table, {write_concurrency, true}]),
    Runs = [ trunc(math:pow(2, X)) || X <- lists:seq(1, 6) ],
    io:format("Table 1 (key per worker, no write concurrency):~n"),
    [ start(X, ?Table1) || X <- Runs ],
    ets:delete(?Table1),
    io:format("Table 1 (key per worker, yes write concurrency):~n"),
    [ start(X, ?Table2) || X <- Runs ],
    ets:delete(?Table2).

start(N, TableName) ->
    Pids = [ spawn_link(fun() -> worker_loop(idle, TableName) end) || _ <- lists:seq(1, N) ],
    M = [ erlang:monitor(process, Pid) || Pid <- Pids ],
    [ erlang:send(Pid, start) || Pid <- Pids ],
    T1 = erlang:monotonic_time(millisecond),
    timer:sleep(?CollectTime),
    [ erlang:send(Pid, stop) || Pid <- Pids],
    T2 = erlang:monotonic_time(millisecond),
    Counts = ets:tab2list(TableName),
    C = lists:foldl(fun ({{_, c}, C}, Acc) -> Acc + C end, 0, Counts),
    receive_downs(M),
    io:format("~p => ~p /s (~p in ~p s)~n", [N, C / ((T2-T1) / 1000), C, (T2-T1)/1000]).

worker_loop(idle, TableName) ->
    receive
        start ->
            worker_loop(start, TableName);
        stop ->
            ok
    end;
worker_loop(start, TableName) ->
    ets:update_counter(TableName, {?SchedulerId(), c}, [{2, 1}], {{?SchedulerId(), c}, 0}),
    receive
        stop ->
            ok
    after 0 ->
              worker_loop(start, TableName)
    end.

receive_downs([]) ->
    ok;
receive_downs([M|Rest]) ->
    receive
        {'DOWN', M, process, _Pid, _Reason} ->
            receive_downs(Rest)
    end.

