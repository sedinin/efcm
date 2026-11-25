%%%-------------------------------------------------------------------
%%% @author sedinin
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Nov 2024 16:44
%%%-------------------------------------------------------------------
-module(efcm_conn).
-author("sedinin").

-behaviour(gen_statem).

-include("efcm.hrl").

%% API
-export([start_link/1, push/3]).

%% gen_statem callbacks
-export([init/1, format_status/1, handle_event/4, terminate/3,
         code_change/4, callback_mode/0]).
%% gen_statem states
-export([open_connection/3, connected/3, down/3]).

%% spawn
-export([reply_errors_and_cancel_timers/2]).

-define(SCOPE, <<"https://www.googleapis.com/auth/firebase.messaging">>).
-define(JSX_OPTS, [return_maps, {labels, atom}]).
-define(HTTP_OPTS, [{timeout, 5000}]).
-define(REQ_OPTS, [{full_result, false}, {body_format, binary}]).
%% timeout is 10 seconds as recommended:
%% https://firebase.google.com/docs/cloud-messaging/scale-fcm
-define(TIMEOUT, 10000).
-define(TOKEN_RETRY, 30000).

%% new
-define(FCM_HOST, "fcm.googleapis.com").
-define(FCM_PORT, 443).


-type stream_data() :: #{ reg_id := binary()
                        , message := term()
                        , from := {pid(), term()}
                        , stream := gun:stream_ref()
                        , timer := reference()
                        , status := non_neg_integer()
                        , headers := gun:req_headers()
                        , body := binary()
                        , retried := boolean() }.

-type state() :: #{ service_file := binary() | string()
                  , push_path := binary()
                  , auth_bearer := binary()
                  , token_tref := reference() | null
                  , gun_pid := pid()
                  , gun_mon := reference()
                  , streams := #{gun:stream_ref() := stream_data()}
                  , max_streams := non_neg_integer() }.

%% todo:
%% 1. renew token (on 401 error)
%% 2. process responses (status + body)
%% 3. some logging
%% 4. reconnects on errors
%% 5. format status

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Creates a gen_statem process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
start_link(ServiceFile) ->
    gen_statem:start_link(?MODULE, [ServiceFile], []).

push(Pid, RegId, Message) ->
    gen_statem:call(Pid, {push, RegId, Message}).


%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================
%% @private
%% @doc This function is called by a gen_statem when it needs to find out
%% the callback mode of the callback module.
-spec callback_mode() -> state_functions.
callback_mode() -> state_functions.

%% @private
%% @doc Whenever a gen_statem is started using gen_statem:start/[3,4] or
%% gen_statem:start_link/[3,4], this function is called by the new
%% process to initialize.
init([ServiceFile]) ->
    {ok, open_connection, #{service_file => ServiceFile, token_tref => null},
     {next_event, internal, init}}.

%% @private
%% @doc Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
format_status(State) ->
    State.

%% @private
%% @doc There should be one instance of this function for each possible
%% state name.  If callback_mode is state_functions, one of these
%% functions is called when gen_statem receives and event from
%% call/2, cast/2, or as a normal process message.
open_connection(internal, _, State0 = #{}) ->
    Opts = #{ protocols => [http2]
            , http2_opts => http2_opts()
            , tls_opts => tls_opts()
            , retry => 0 },
    {ok, GunPid} = gun:open(?FCM_HOST, ?FCM_PORT, Opts),
    GunMon = monitor(process, GunPid),
    %% got connection, now we have to reload access token
    State1 = reload_access_token(State0),
    %% initially one stream allowed
    State2 = State1#{ gun_pid => GunPid
                    , gun_mon => GunMon
                    , streams => #{}
                    , max_streams => 1 },
    {next_state, connected, State2}.


-spec connected(_, _, state()) -> _.
connected({call, From}, {push, RegId, Message}, State0 = #{}) ->
    case send_push(From, RegId, Message, false, State0) of
        {error, Reason} ->
            {keep_state_and_data, {reply, From, {error, Reason}}};
        {ok, State1} ->
            {keep_state, State1}
    end;
connected(info, refresh_token, State0) ->
    %% scheduled access token renewal
    {keep_state, refresh_access_token(State0)};
connected(info
         , {gun_response, GunPid, StreamRef, fin, 401, _Headers}
         , #{gun_pid := GunPid} = State0) ->
    %% got 401, refresh token then repeat request (send push)
    gun:cancel(GunPid, StreamRef),
    case take_stream(StreamRef, State0) of
        {StreamData, State1} -> retry_unauthorized(StreamData, State1);
        error -> {keep_state, State0}
    end;
connected(info
         , {gun_response, GunPid, StreamRef, fin, Status, Headers}
         , #{gun_pid := GunPid} = State0) ->
    %% got final response without body
    lager:debug("final response without body, status: ~B, headers: ~p", [Status, Headers]),
    gun:cancel(GunPid, StreamRef), %% final response, closing stream
    case take_stream(StreamRef, State0) of
        {#{from := From}, State1} ->
            gen_statem:reply(From, {Status, Headers, no_body}),
            {keep_state, State1};
        error ->
            {keep_state, State0}
    end;
connected(info
         , {gun_response, GunPid, StreamRef, nofin, Status, Headers}
         , #{gun_pid := GunPid} = State0) ->
    %% non-final response without body
    lager:debug("non-final response without body, status: ~B, headers: ~p", [Status, Headers]),
    {keep_state, update_stream(StreamRef,
                               fun(SD) -> SD#{status => Status, headers => Headers} end,
                               State0)};
connected(info
         , {gun_data, GunPid, StreamRef, fin, Data}
         , #{gun_pid := GunPid} = State0) ->
    %% got data, finally
    case take_stream(StreamRef, State0) of
        {#{status := 401} = StreamData, State1} ->
            retry_unauthorized(StreamData, State1);
        {StreamData, State1} ->
            lager:debug("final data response, data: ~p, stream data: ~p", [Data, StreamData]),
            #{from := From, status := Status, body := B0, reg_id := PushId} = StreamData,
            gen_statem:reply(From, data_reply(Status, <<B0/binary, Data/binary>>, PushId)),
            {keep_state, State1};
        error ->
            %% stream already answered (timeout?), ignoring
            {keep_state, State0}
    end;
connected(info
         , {gun_data, GunPid, StreamRef, nofin, Data}
         , #{gun_pid := GunPid} = State0) ->
    %% add data to buffer, still waiting
    lager:debug("non-final data response, data: ~p", [Data]),
    {keep_state, update_stream(StreamRef,
                               fun(#{body := B0} = SD) -> SD#{body => <<B0/binary, Data/binary>>} end,
                               State0)};
connected(info
         , {gun_error, GunPid, StreamRef, Reason}
         , #{gun_pid := GunPid} = State0) ->
    %% answering with error, remove entry
    case take_stream(StreamRef, State0) of
        {StreamData, State1} ->
            lager:debug("stream error received, error: ~p, stream data: ~p", [Reason, StreamData]),
            #{from := From} = StreamData,
            gen_statem:reply(From, {error, Reason}),
            gun:cancel(GunPid, StreamRef),
            {keep_state, State1};
        error ->
            lager:debug("stream error received, error: ~p, no stream data", [Reason]),
            {keep_state, State0}
    end;
connected(info
         , {gun_error, GunPid, Reason}
         , #{gun_pid := GunPid} = State0) ->
    %% answer with error for all streams, remove all entries, going to reconnect
    #{streams := Streams} = State0,
    lager:debug("gun error on connection received: ~p, streams count: ~B", [Reason, maps:size(Streams)]),
    spawn(efcm_conn, reply_errors_and_cancel_timers, [Streams, Reason]),
    {next_state, down, State0#{streams => #{}, max_streams => 1},
     {next_event, internal, {down, ?FUNCTION_NAME, Reason}}};
connected(info
         , {timeout, GunPid, StreamRef}
         , #{gun_pid := GunPid} = State0) ->
    %% gun pid matches, we have to answer {error, timeout}
    case take_stream(StreamRef, State0) of
        {StreamData, State1} ->
            lager:debug("timeout signalled on stream: ~p", [StreamData]),
            #{from := From} = StreamData,
            gen_statem:reply(From, {error, timeout}),
            gun:cancel(GunPid, StreamRef),
            {keep_state, State1};
        error ->
            %% may be just answered and removed, ignoring
            {keep_state, State0}
    end;
connected(info,
          {timeout, GotGunPid, _StreamRef},
          State0) ->
    %% timeout from different connection?
    %% ignoring
    #{gun_pid := MyGunPid} = State0,
    lager:debug("timeout from different connection: ~p, mine: ~p", [GotGunPid, MyGunPid]),
    {keep_state, State0};
connected(info
         , {gun_notify, GunPid, settings_changed, Settings}
         , #{gun_pid := GunPid} = State0) ->
    #{max_streams := MaxStreams0} = State0,
    %% settings received, if contains max_concurrent_streams, update it
    lager:debug("gun settings changed received: ~p", [Settings]),
    MaxStreams1 = maps:get(max_concurrent_streams, Settings, MaxStreams0),
    lager:debug("new max streams: ~p", [MaxStreams1]),
    {keep_state, State0#{max_streams => MaxStreams1}};
connected(EventType, EventContent, StateData) ->
    handle_common(EventType, EventContent, ?FUNCTION_NAME, StateData, drop).


-spec down(_, _, _) -> _.
down(internal
    , _
    , #{gun_pid := GunPid
       , gun_mon := GunMon
       }) ->
    lager:debug("down event received for gun pid: ~p", [GunPid]),
    true = demonitor(GunMon, [flush]),
    gun:close(GunPid),
    {keep_state_and_data, {state_timeout, 500, backoff}}; %% sleep 500ms before reconnect
down(state_timeout, backoff, State) ->
    {next_state, open_connection, State,
     {next_event, internal, init}};
down(EventType, EventContent, StateData) ->
    lager:debug("unknown (common) event received, type: ~p, content: ~p", [EventType, EventContent]),
    handle_common(EventType, EventContent, ?FUNCTION_NAME, StateData, postpone).


-spec handle_common(_, _, _, _, _) -> _.
handle_common({call, From}, gun_pid, _, #{gun_pid := GunPid}, _) ->
    {keep_state_and_data, {reply, From, GunPid}};
handle_common(cast, stop, _, _, _) ->
    {stop, normal};
handle_common(info
             , {'DOWN', GunMon, process, GunPid, Reason}
             , StateName
             , #{gun_pid := GunPid, gun_mon := GunMon} = State0
             , _) ->
    %% gun died, answering with errors, cleanup entries, reconnect
    lager:debug("gun process ~p died with reason: ~p", [GunPid, Reason]),
    #{streams := Streams} = State0,
    spawn(efcm_conn, reply_errors_and_cancel_timers, [Streams, Reason]),
    {next_state, down, State0#{streams => #{}, max_streams => 1},
     {next_event, internal, {down, StateName, Reason}}};
handle_common(state_timeout
             , EventContent
             , StateName
             , #{gun_pid := GunPid} = State
             , _) ->
    lager:debug("state (~p) timeout on gun connection: ~p, event: ~p",
                [StateName, GunPid, EventContent]),
    gun:close(GunPid),
    {next_state, down, State,
     {next_event, internal, {state_timeout, StateName, EventContent}}};
%% xxx: WTF?
handle_common(_, _, _, _, postpone) ->
    {keep_state_and_data, postpone};
handle_common(_, _, _, _, drop) ->
    keep_state_and_data.


%% @private
%% @doc If callback_mode is handle_event_function, then whenever a
%% gen_statem receives an event from call/2, cast/2, or as a normal
%% process message, this function is called.
handle_event(_EventType, _EventContent, _StateName, State = #{}) ->
    NextStateName = the_next_state_name,
    {next_state, NextStateName, State}.

%% @private
%% @doc This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
terminate(_Reason, _StateName, _State = #{}) ->
    ok.

%% @private
%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State = #{}, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec(append_token(map(), binary()) -> map()).
append_token(#{message := Message} = M, Token) ->
    M#{message => Message#{token => Token}};
append_token(#{<<"message">> := Message} = M, Token) ->
    M#{message => Message#{token => Token}};
append_token(Message, Token) ->
    #{message => Message#{token => Token}}.


-spec(push(GunPid :: pid(), AuthKey :: binary(), PushPath :: binary(),
           RegId :: binary(), Message0 :: map()) -> {ok, gun:stream_ref()}).
push(GunPid, AuthKey, PushPath, RegId, Message0) ->
    MapBody = append_token(Message0, RegId),
    Body = jsx:encode(MapBody),
    Headers = http_headers(AuthKey),
    gun:post(GunPid, PushPath, Headers, Body).

-spec send_push({pid(), term()}, binary(), term(), boolean(), state()) ->
          {ok, state()} | {error, {overload, non_neg_integer(), non_neg_integer()}}.
send_push(From, RegId, Message, Retried, State) ->
    #{gun_pid := GunPid
     , push_path := PushPath
     , auth_bearer := AuthBearer
     , streams := Streams0
     , max_streams := MaxStreams} = State,
    TotalStreams = maps:size(Streams0),
    case stream_allowed(TotalStreams, MaxStreams) of
        false ->
            lager:debug("no more streams allowed, current: ~p, max: ~p", [TotalStreams, MaxStreams]),
            {error, {overload, TotalStreams, MaxStreams}};
        true ->
            StreamRef = push(GunPid, AuthBearer, PushPath, RegId, Message),
            Tmr = erlang:send_after(?TIMEOUT, self(), {timeout, GunPid, StreamRef}),
            StreamData = #{ reg_id => RegId
                          , message => Message
                          , from => From
                          , stream => StreamRef
                          , timer => Tmr
                          , status => 200 %% b4 we know real status
                          , headers => []
                          , body => <<>>
                          , retried => Retried },
            Streams1 = Streams0#{StreamRef => StreamData},
            {ok, State#{streams => Streams1}}
    end.


%% removes stream data from state and cancels its timeout timer
-spec take_stream(gun:stream_ref(), state()) -> {stream_data(), state()} | error.
take_stream(StreamRef, #{streams := Streams0} = State) ->
    case maps:take(StreamRef, Streams0) of
        {#{timer := Tmr} = StreamData, Streams1} ->
            erlang:cancel_timer(Tmr),
            {StreamData, State#{streams => Streams1}};
        error ->
            error
    end.


%% applies F to stream data, if stream is still known
-spec update_stream(gun:stream_ref(), fun((stream_data()) -> stream_data()), state()) -> state().
update_stream(StreamRef, F, #{streams := Streams0} = State) ->
    case maps:find(StreamRef, Streams0) of
        {ok, StreamData} -> State#{streams => Streams0#{StreamRef => F(StreamData)}};
        error -> State
    end.


%% got 401: refresh token and resend push once
retry_unauthorized(#{retried := true, from := From}, State) ->
    gen_statem:reply(From, {error, unauthorized}),
    {keep_state, State};
retry_unauthorized(#{from := From, reg_id := RegId, message := Message}, State0) ->
    State1 = refresh_access_token(State0),
    case send_push(From, RegId, Message, true, State1) of
        {error, Reason} ->
            gen_statem:reply(From, {error, Reason}),
            {keep_state, State1};
        {ok, State2} ->
            {keep_state, State2}
    end.


data_reply(200, Body, _PushId) ->
    %% response is 'ok', parsing json body and return result id
    try jsx:decode(Body, ?JSX_OPTS) of
        #{name := Name} ->
            {ok, lists:last(binary:split(Name, <<"/">>, [global, trim_all]))};
        _ ->
            {error, {bad_response, Body}}
    catch _:_ ->
            {error, {bad_response, Body}}
    end;
data_reply(404, _Body, PushId) ->
    %% wrong (expired?) push id, specific error
    lager:debug("push id not found: ~s", [PushId]),
    {error, wrong_push_id};
data_reply(Status, Body, _PushId) ->
    %% unknown status, replying with error
    lager:debug("unknown error: ~B : ~p", [Status, Body]),
    {error, {Status, Body}}.


-spec(make_fcm_push_path(binary()) -> binary()).
make_fcm_push_path(ProjectId) ->
    iolist_to_binary(["/v1/projects/", ProjectId, "/messages:send"]).


reload_access_token(#{service_file := ServiceFile} = State0) ->
    lager:debug("reloading access token"),
    cancel_timer(State0),
    lager:debug("loading google file: ~p", [ServiceFile]),
    {ok, Bin} = file:read_file(ServiceFile),
    #{project_id := ProjectId} = jsx:decode(Bin, ?JSX_OPTS),
    {ok, #{access_token := AccessToken}} = google_oauth:get_access_token(ServiceFile, ?SCOPE),
    AuthorizationBearer = <<"Bearer ", AccessToken/binary>>,
    State0#{
            push_path => make_fcm_push_path(ProjectId),
            auth_bearer => AuthorizationBearer,
            token_tref => erlang:send_after(timer:seconds(3540), self(), refresh_token)
           }.


%% like reload_access_token/1, but keeps old token and retries later on failure
refresh_access_token(State0) ->
    try
        reload_access_token(State0)
    catch C:E ->
            lager:warning("FCM access token refresh failed: ~p:~p, retry in ~B ms",
                          [C, E, ?TOKEN_RETRY]),
            State0#{token_tref => erlang:send_after(?TOKEN_RETRY, self(), refresh_token)}
    end.


cancel_timer(#{token_tref := null}) ->
    ok;
cancel_timer(#{token_tref := TimerRef}) ->
    erlang:cancel_timer(TimerRef).


http_headers(AuthKey) ->
    [{<<"authorization">>, AuthKey}
    , {<<"content-type">>, <<"application/json; UTF-8">>}].


http2_opts() -> #{notify_settings_changed => true}.


tls_opts() ->
    [{verify, verify_peer}
    , {cacerts, public_key:cacerts_get()}
    , {customize_hostname_check,
       [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}].


-spec(stream_allowed(StreamsCount :: non_neg_integer(),
                     MaxStreams :: non_neg_integer() | infinity) ->
             boolean()).
stream_allowed(_StreamsCount, infinity) -> true;
stream_allowed(StreamsCount, MaxStreams) ->
    StreamsCount < MaxStreams.

%%%===================================================================
%%% spawn/3 functions
%%%===================================================================
-spec reply_errors_and_cancel_timers(map(), term()) -> ok.
reply_errors_and_cancel_timers(Streams, Reason) ->
    [reply_error_and_cancel_timer(From, Reason, Tmr) ||
        #{from := From, timer := Tmr} <- maps:values(Streams)],
    ok.


-spec reply_error_and_cancel_timer(From :: {pid(), term()}, Reason :: term(),
                                   Tmr :: reference()) -> ok.
reply_error_and_cancel_timer(From, Reason, Tmr) ->
    erlang:cancel_timer(Tmr),
    gen_statem:reply(From, {error, Reason}).
