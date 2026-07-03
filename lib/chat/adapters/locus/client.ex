defmodule Chat.Adapters.Locus.Client do
  @moduledoc """
  A minimal RESP2 client over `:gen_tcp` — the engine's own wire to a Locus
  server, with **zero dependencies** (the architectural firewall forbids
  `:redix` and friends; Locus speaks the Redis protocol, and RESP2 is ~100
  lines, so the adapter carries its own).

  One process = one TCP connection, used in **passive** mode with a private
  parse buffer. All traffic runs inside `handle_call/3`, so a multi-step
  protocol (`WATCH … MULTI … EXEC`) executed via `exclusive/2` can never
  interleave with another caller on the same socket — which is exactly the
  guarantee optimistic transactions need.

  Connections are lazy (first use) and self-healing: any socket error closes
  the connection and surfaces `{:error, reason}`; the next call reconnects.
  """
  use GenServer

  @connect_timeout 3_000
  @recv_timeout 5_000
  @call_timeout 10_000

  @type reply :: binary() | integer() | nil | [reply()] | {:error, binary()}

  # ── API ─────────────────────────────────────────────────────────────────────

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Run one command; returns the decoded reply (Redis errors as `{:error, msg}`)."
  @spec command(GenServer.server(), [iodata()]) :: {:ok, reply()} | {:error, term()}
  def command(server, cmd), do: GenServer.call(server, {:pipeline, [cmd]}, @call_timeout) |> one()

  @doc "Run several commands in one round trip; replies in order."
  @spec pipeline(GenServer.server(), [[iodata()]]) :: {:ok, [reply()]} | {:error, term()}
  def pipeline(server, cmds), do: GenServer.call(server, {:pipeline, cmds}, @call_timeout)

  @doc """
  Run a multi-step protocol with the socket held exclusively. `fun` receives a
  runner `([[iodata()]] -> {:ok, [reply()]} | {:error, term()})` and may call it
  several times (e.g. `WATCH`+reads, then decide, then `MULTI…EXEC`). If `fun`
  raises or a step fails at the transport level, the connection is dropped so
  no `WATCH`/`MULTI` state can leak into later calls.
  """
  @spec exclusive(GenServer.server(), (fun -> result)) :: result | {:error, term()}
        when result: term()
  def exclusive(server, fun), do: GenServer.call(server, {:exclusive, fun}, @call_timeout)

  defp one({:ok, [r]}), do: {:ok, r}
  defp one({:error, _} = e), do: e

  # ── GenServer ───────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    state = %{
      sock: nil,
      buf: <<>>,
      host: Keyword.get(opts, :host, ~c"127.0.0.1"),
      port: Keyword.get(opts, :port, 6379),
      password: Keyword.get(opts, :password)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:pipeline, cmds}, _from, state) do
    case with_conn(state, fn st -> run_pipeline(st, cmds) end) do
      {:ok, replies, st} -> {:reply, {:ok, replies}, st}
      {:error, reason, st} -> {:reply, {:error, reason}, st}
    end
  end

  def handle_call({:exclusive, fun}, _from, state) do
    case ensure_conn(state) do
      {:error, reason, st} ->
        {:reply, {:error, reason}, st}

      {:ok, st0} ->
        # The runner threads socket/buffer state through the process dictionary
        # for the duration of this call — private to this process, and reset in
        # `after`. A transport error or a raise nukes the connection so no
        # transaction state survives onto the next caller.
        Process.put(:locus_x, {st0, :ok})

        try do
          runner = fn cmds ->
            {st, health} = Process.get(:locus_x)

            case health do
              :broken ->
                {:error, :closed}

              :ok ->
                case run_pipeline(st, cmds) do
                  {:ok, replies, st2} ->
                    Process.put(:locus_x, {st2, :ok})
                    {:ok, replies}

                  {:error, reason, st2} ->
                    Process.put(:locus_x, {st2, :broken})
                    {:error, reason}
                end
            end
          end

          result = fun.(runner)
          {st, health} = Process.get(:locus_x)
          st = if health == :broken, do: drop(st), else: st
          {:reply, result, st}
        rescue
          e ->
            {st, _} = Process.get(:locus_x)
            {:reply, {:error, {:exclusive_raised, e}}, drop(st)}
        after
          Process.delete(:locus_x)
        end
    end
  end

  # ── Connection ──────────────────────────────────────────────────────────────

  defp with_conn(state, fun) do
    case ensure_conn(state) do
      {:ok, st} -> fun.(st)
      {:error, reason, st} -> {:error, reason, st}
    end
  end

  defp ensure_conn(%{sock: nil} = state) do
    host = if is_binary(state.host), do: String.to_charlist(state.host), else: state.host

    with {:ok, sock} <-
           :gen_tcp.connect(
             host,
             state.port,
             [:binary, active: false, nodelay: true],
             @connect_timeout
           ),
         st = %{state | sock: sock, buf: <<>>},
         {:ok, st} <- maybe_auth(st) do
      {:ok, st}
    else
      {:error, reason} -> {:error, {:connect, reason}, %{state | sock: nil, buf: <<>>}}
      {:error, reason, st} -> {:error, reason, drop(st)}
    end
  end

  defp ensure_conn(state), do: {:ok, state}

  defp maybe_auth(%{password: nil} = st), do: {:ok, st}
  defp maybe_auth(%{password: ""} = st), do: {:ok, st}

  defp maybe_auth(%{password: pw} = st) do
    case run_pipeline(st, [["AUTH", pw]]) do
      {:ok, ["OK"], st} -> {:ok, st}
      {:ok, [{:error, msg}], st} -> {:error, {:auth, msg}, st}
      {:error, reason, st} -> {:error, reason, st}
    end
  end

  defp drop(%{sock: nil} = st), do: st

  defp drop(%{sock: sock} = st) do
    :gen_tcp.close(sock)
    %{st | sock: nil, buf: <<>>}
  end

  # ── Wire ────────────────────────────────────────────────────────────────────

  defp run_pipeline(st, cmds) do
    payload = Enum.map(cmds, &encode/1)

    case :gen_tcp.send(st.sock, payload) do
      :ok -> recv_replies(st, length(cmds), [])
      {:error, reason} -> {:error, {:send, reason}, drop(st)}
    end
  end

  defp recv_replies(st, 0, acc), do: {:ok, Enum.reverse(acc), st}

  defp recv_replies(st, n, acc) do
    case recv_reply(st) do
      {:ok, reply, st} -> recv_replies(st, n - 1, [reply | acc])
      {:error, reason, st} -> {:error, reason, st}
    end
  end

  defp recv_reply(st) do
    case parse(st.buf) do
      {:ok, reply, rest} ->
        {:ok, reply, %{st | buf: rest}}

      :more ->
        case :gen_tcp.recv(st.sock, 0, @recv_timeout) do
          {:ok, data} -> recv_reply(%{st | buf: st.buf <> data})
          {:error, reason} -> {:error, {:recv, reason}, drop(st)}
        end
    end
  end

  @doc false
  def encode(cmd) do
    parts = Enum.map(cmd, &IO.iodata_to_binary(to_arg(&1)))
    ["*", Integer.to_string(length(parts)), "\r\n" | Enum.map(parts, &encode_bulk/1)]
  end

  defp to_arg(i) when is_integer(i), do: Integer.to_string(i)
  defp to_arg(a) when is_atom(a), do: Atom.to_string(a)
  defp to_arg(b), do: b

  defp encode_bulk(b), do: ["$", Integer.to_string(byte_size(b)), "\r\n", b, "\r\n"]

  # RESP2 incremental parser: {:ok, reply, rest} | :more
  @doc false
  def parse(<<"+", rest::binary>>), do: parse_line(rest, & &1)
  def parse(<<"-", rest::binary>>), do: parse_line(rest, &{:error, &1})
  def parse(<<":", rest::binary>>), do: parse_line(rest, &String.to_integer/1)

  def parse(<<"$", rest::binary>>) do
    with {:ok, len, rest} <- parse_int_line(rest) do
      cond do
        len < 0 -> {:ok, nil, rest}
        byte_size(rest) >= len + 2 -> bulk_body(rest, len)
        true -> :more
      end
    end
  end

  def parse(<<"*", rest::binary>>) do
    with {:ok, n, rest} <- parse_int_line(rest) do
      if n < 0, do: {:ok, nil, rest}, else: parse_elems(rest, n, [])
    end
  end

  def parse(<<>>), do: :more
  def parse(_), do: {:ok, {:error, "protocol desync"}, <<>>}

  defp bulk_body(rest, len) do
    <<body::binary-size(^len), "\r\n", tail::binary>> = rest
    {:ok, body, tail}
  end

  defp parse_elems(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp parse_elems(rest, n, acc) do
    case parse(rest) do
      {:ok, el, rest} -> parse_elems(rest, n - 1, [el | acc])
      :more -> :more
    end
  end

  defp parse_line(bin, wrap) do
    case :binary.split(bin, "\r\n") do
      [line, rest] -> {:ok, wrap.(line), rest}
      _ -> :more
    end
  end

  defp parse_int_line(bin) do
    case :binary.split(bin, "\r\n") do
      [line, rest] -> {:ok, String.to_integer(line), rest}
      _ -> :more
    end
  end
end
