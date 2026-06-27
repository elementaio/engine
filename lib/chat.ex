defmodule Chat do
  @moduledoc """
  Friendly top-level API for driving the engine core.

  A thin convenience layer over the configured ports and runtime — handy in IEx,
  for the body, and for tests. Covers conversations & membership, the durable
  message log, and presence. Live message flow goes through `Chat.Session`
  (started by the edge per connection).

      {:ok, _} = Chat.Adapters.InMemory.Persistence.start_link([])
      {:ok, _} = Chat.Adapters.InMemory.ConversationStore.start_link([])

      :ok = Chat.create_conversation("team", ["alice", "bob", "carol"])
      :ok = Chat.add_member("team", "dave")   # live: online dave starts receiving
      3   = Chat.member_count("team") |> elem(1)  # (after a remove, etc.)
  """
  alias Chat.{Envelope, Message, Ports, Types}

  # ── Conversations / membership ──────────────────────────────────────────────

  @doc "Define a conversation with an initial member set (no join announcements)."
  @spec create_conversation(Types.conversation_id(), [Types.user_id()]) :: :ok
  def create_conversation(conversation_id, member_user_ids) when is_list(member_user_ids) do
    Enum.each(member_user_ids, &do_add(conversation_id, &1, announce: false))
  end

  @doc "Add a member at runtime: subscribe their online sessions and announce a join."
  @spec add_member(Types.conversation_id(), Types.user_id()) :: :ok
  def add_member(conversation_id, user_id), do: do_add(conversation_id, user_id, announce: true)

  @doc "Remove a member: unsubscribe their online sessions and announce a leave."
  @spec remove_member(Types.conversation_id(), Types.user_id()) :: :ok
  def remove_member(conversation_id, user_id) do
    :ok = Ports.conversation_store().remove_member(conversation_id, user_id)

    # Announce the leave while the member is still subscribed (so they're told
    # they were removed), THEN unsubscribe them from future fan-out.
    announce(conversation_id, :leave, user_id)

    for pid <- Chat.Router.sessions_for(user_id) do
      safe_session_call(pid, fn -> Chat.Session.unsubscribe(pid, conversation_id) end)
    end

    invalidate_size(conversation_id)
    :ok
  end

  @doc "List the members of a conversation."
  @spec members(Types.conversation_id()) :: [Types.user_id()]
  def members(conversation_id), do: Chat.ConversationStore.members(conversation_id)

  @doc "Member count (cheap)."
  @spec member_count(Types.conversation_id()) :: {:ok, non_neg_integer()} | {:error, term()}
  def member_count(conversation_id), do: Ports.conversation_store().member_count(conversation_id)

  @doc "Is the user connected on at least one device right now?"
  @spec online?(Types.user_id()) :: boolean()
  def online?(user_id), do: Chat.Router.sessions_for(user_id) != []

  @doc "Presence of a user: `:online` or `{:offline, last_seen_ms | nil}`."
  @spec presence_of(Types.user_id()) :: :online | {:offline, integer() | nil}
  def presence_of(user_id) do
    if Chat.Presence.online?(user_id) do
      :online
    else
      case Ports.presence_store().last_seen(user_id) do
        {:ok, ts} -> {:offline, ts}
        _ -> {:offline, nil}
      end
    end
  end

  @doc "Aggregate group read state at `seq` as `{count, readers}` — the seen-by-N count."
  @spec read_state(Types.conversation_id(), Types.seq()) :: {non_neg_integer(), [Types.user_id()]}
  def read_state(conversation_id, seq), do: Chat.Receipts.aggregate(conversation_id, seq)

  # ── Durable message log ─────────────────────────────────────────────────────

  @doc """
  Durably append a message to a conversation and get its assigned `seq`.
  Idempotent on `message.id`: re-appending the same id returns the same `seq`.
  """
  @spec append(Message.t(), Types.conversation_id()) :: {:ok, Types.seq()} | {:error, term()}
  def append(%Message{} = message, conversation_id) do
    Ports.persistence().append(conversation_id, message)
  end

  @doc "Read messages after `after_seq` (catch-up by sequence), oldest first."
  @spec history(Types.conversation_id(), Types.seq(), pos_integer()) ::
          {:ok, [Message.t()]} | {:error, term()}
  def history(conversation_id, after_seq \\ 0, limit \\ 100) do
    Ports.persistence().read_after(conversation_id, after_seq, limit)
  end

  @typedoc """
  One page of history: up to `limit` messages oldest-first, the `next_after` cursor
  to pass back for the following page, and `more?` (is there at least one message
  beyond this page). When `more?` is false, `next_after` is the last seq seen (or
  the requested `after_seq` if the page was empty) — re-requesting with it is a
  safe no-op.
  """
  @type page :: %{messages: [Message.t()], next_after: Types.seq(), more?: boolean()}

  @doc """
  Read one page of history after `after_seq` with a continuation cursor.

  Paginate by re-calling with `next_after` until `more?` is false. Built on
  `read_after` with a `limit + 1` look-ahead, so detecting "is there more" costs no
  extra round-trip and needs nothing from the adapter beyond the existing contract.
  """
  @spec history_page(Types.conversation_id(), Types.seq(), pos_integer()) ::
          {:ok, page()} | {:error, term()}
  def history_page(conversation_id, after_seq \\ 0, limit \\ 100) when limit > 0 do
    case Ports.persistence().read_after(conversation_id, after_seq, limit + 1) do
      {:ok, msgs} ->
        more? = length(msgs) > limit
        page = if more?, do: Enum.take(msgs, limit), else: msgs

        next_after =
          case List.last(page) do
            %Message{seq: seq} -> seq
            nil -> after_seq
          end

        {:ok, %{messages: page, next_after: next_after, more?: more?}}

      {:error, _} = err ->
        err
    end
  end

  @doc "Highest assigned seq for a conversation (0 if empty)."
  @spec latest_seq(Types.conversation_id()) :: {:ok, Types.seq()} | {:error, term()}
  def latest_seq(conversation_id) do
    Ports.persistence().latest_seq(conversation_id)
  end

  @doc """
  Publish a message into a channel from a non-session publisher (e.g. an
  ingestion service). Assigns seq, persists, and fans out live to all watchers.
  """
  @spec inject(Types.conversation_id(), Message.t()) ::
          {:ok, Types.seq() | :ephemeral} | {:error, term()}
  def inject(conversation_id, %Message{} = message),
    do: Chat.Conversation.inject(conversation_id, message)

  # ── Health & lifecycle ──────────────────────────────────────────────────────

  @doc "Is this node ready to accept new connections? (config valid and not draining)."
  @spec ready?() :: boolean()
  def ready?, do: Chat.Health.ready?()

  @doc "Begin a graceful drain: refuse new sessions, keep existing ones (for rolling a node)."
  @spec drain() :: :ok
  def drain, do: Chat.Health.drain()

  @doc "Resume accepting new sessions after a drain."
  @spec resume() :: :ok
  def resume, do: Chat.Health.resume()

  # ── internals ───────────────────────────────────────────────────────────────

  defp do_add(conversation_id, user_id, opts) do
    :ok = Ports.conversation_store().add_member(conversation_id, user_id)

    # Live-subscribe any sessions this user already has connected.
    for pid <- Chat.Router.sessions_for(user_id) do
      safe_session_call(pid, fn -> Chat.Session.subscribe(pid, conversation_id) end)
    end

    invalidate_size(conversation_id)
    if Keyword.get(opts, :announce, false), do: announce(conversation_id, :join, user_id)
    :ok
  end

  # A session can die between `:syn` listing it and our call (sessions are
  # ephemeral and churn constantly). A dying device must NOT crash a membership
  # change for the whole conversation — it will resubscribe on reconnect. Tolerate
  # the dead-process exit.
  defp safe_session_call(_pid, fun) do
    fun.()
  catch
    :exit, _ -> :ok
  end

  # A live, ephemeral system notification to online members (not persisted in M2).
  defp announce(conversation_id, event, subject_user) do
    Chat.Fanout.dispatch(conversation_id, %Envelope{
      type: :system,
      conversation_id: conversation_id,
      status: event,
      sender_id: subject_user
    })
  end

  defp invalidate_size(conversation_id) do
    case Registry.lookup(Chat.ConversationRegistry, conversation_id) do
      [{pid, _}] -> GenServer.cast(pid, :invalidate_size)
      [] -> :ok
    end
  end
end
