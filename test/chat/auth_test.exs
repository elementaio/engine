defmodule Chat.AuthTest do
  @moduledoc """
  P0 security: authentication on connect (credentials path) and per-verb
  authorization. The verdict comes from the configured Auth adapter; the engine
  enforces it without crashing.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Session}

  # A strict adapter: only `{token: "good", as: uid}` authenticates; trusted
  # binary user_ids pass through; authorization denies everything by default.
  defmodule StrictAuth do
    @behaviour Chat.Auth.Port
    @impl true
    def authenticate(%{token: "good", as: uid}) when is_binary(uid), do: {:ok, uid}
    def authenticate(uid) when is_binary(uid) and uid != "", do: {:ok, uid}
    def authenticate(_), do: {:error, :bad_credentials}

    @impl true
    # allow only the conversation literally named "ok"; deny the rest.
    def authorize(_action, _user, "ok"), do: :ok
    def authorize(_action, _user, _resource), do: {:error, :forbidden}
  end

  setup do
    ensure_clean(Persistence)
    ensure_clean(ConversationStore)
    prev = Application.get_env(:chat_engine, :auth_adapter)
    Application.put_env(:chat_engine, :auth_adapter, StrictAuth)

    on_exit(fn ->
      Application.put_env(:chat_engine, :auth_adapter, prev)
      drain_dynamic_supervisors()
    end)

    :ok
  end

  test "connect with bad credentials is refused" do
    assert {:error, {:unauthenticated, :bad_credentials}} =
             Session.connect(%{
               credentials: %{token: "bad"},
               device_id: "d",
               transport: {TestTransport, {self(), :x}}
             })
  end

  test "connect with good credentials authenticates and derives the user_id" do
    assert {:ok, pid} =
             Session.connect(%{
               credentials: %{token: "good", as: "alice"},
               device_id: "d",
               transport: {TestTransport, {self(), :alice}}
             })

    assert is_pid(pid)
  end

  test "a trusted user_id bypasses authentication (in-VM body path)" do
    assert {:ok, pid} =
             Session.connect(%{
               user_id: "bob",
               device_id: "d",
               transport: {TestTransport, {self(), :bob}}
             })

    assert is_pid(pid)
  end

  test "forbidden :send pushes :error and persists nothing" do
    :ok = Chat.create_conversation("nope", ["A"])
    a = connect("A", :A)

    Session.handle_inbound(a, %Envelope{
      type: :send,
      conversation_id: "nope",
      id: "m1",
      payload: "x"
    })

    Session.sync(a)

    assert_receive {:frame, :A, %Envelope{type: :error, id: "m1", reason: :forbidden}}
    refute_received {:frame, :A, %Envelope{type: :ack}}
    assert {:ok, 0} = Chat.latest_seq("nope")
  end

  test "authorized :send (allowed conversation) succeeds" do
    :ok = Chat.create_conversation("ok", ["A"])
    a = connect("A", :A)

    Session.handle_inbound(a, %Envelope{
      type: :send,
      conversation_id: "ok",
      id: "m1",
      payload: "x"
    })

    Session.sync(a)

    assert_receive {:frame, :A, %Envelope{type: :ack, id: "m1", seq: 1}}
  end

  test "forbidden :sync pushes :error and leaks no history" do
    :ok = Chat.create_conversation("nope", ["A"])

    {:ok, _} =
      Persistence.append("nope", %Chat.Message{id: "s1", sender_id: "x", payload: "secret"})

    a = connect("A", :A)

    Session.handle_inbound(a, %Envelope{type: :sync, conversation_id: "nope", seq: 0})
    Session.sync(a)

    assert_receive {:frame, :A, %Envelope{type: :error, reason: :forbidden}}
    refute_received {:frame, :A, %Envelope{type: :sync_page}}
  end

  defp connect(user, label) do
    {:ok, pid} =
      Session.connect(%{
        user_id: user,
        device_id: to_string(label),
        transport: {TestTransport, {self(), label}}
      })

    pid
  end

  defp ensure_clean(mod) do
    case start_supervised(mod) do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end

    mod.reset()
  end

  defp drain_dynamic_supervisors do
    for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
        {_, pid, _, _} <- DynamicSupervisor.which_children(sup),
        is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end
  end
end
