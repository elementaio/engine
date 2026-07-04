defmodule Chat.Config do
  @moduledoc """
  Boot-time configuration validation (OBS-3).

  The engine is wired entirely through `:chat_engine` application config: each
  port points at a concrete adapter, and a handful of numeric knobs tune
  backpressure and ephemeral-signal fan-out. A misconfigured body (a missing
  adapter, a module that doesn't implement the behaviour, a nonsensical limit)
  should fail LOUDLY at boot — not with a confusing crash on the first message
  hours later.

  `validate!/0` runs from `Chat.Application.start/2` before any child is started,
  so the node refuses to boot on bad config. It checks, for each REQUIRED port,
  that an adapter is configured, that its module is loadable, and that it exports
  every (non-optional) callback of the port behaviour; the optional `OfflineQueue`
  port is validated only when set. It also range-checks the numeric knobs.
  """

  # {config key, port behaviour, required?}
  @ports [
    {:persistence_adapter, Chat.Persistence.Port, true},
    {:conversation_store_adapter, Chat.ConversationStore.Port, true},
    {:cursor_store_adapter, Chat.CursorStore.Port, true},
    {:presence_store_adapter, Chat.PresenceStore.Port, true},
    {:receipt_store_adapter, Chat.ReceiptStore.Port, true},
    {:auth_adapter, Chat.Auth.Port, true},
    {:offline_queue_adapter, Chat.OfflineQueue.Port, false}
  ]

  # Knobs that, when set, must be a positive integer.
  @positive_int_keys [
    :presence_max,
    :typing_max,
    :max_mailbox,
    :max_payload_bytes,
    :offline_push_max_members,
    :sync_page_max,
    :max_pending_gap,
    :offline_max_inflight,
    :offline_recheck_ms
  ]

  @doc """
  Validate `:chat_engine` config. Returns `:ok` or raises `ArgumentError` with
  every problem found (not just the first), so one boot surfaces them all.
  """
  @spec validate!() :: :ok
  def validate! do
    problems =
      Enum.flat_map(@ports, &port_problems/1) ++
        Enum.flat_map(@positive_int_keys, &int_problems/1) ++
        fence_problems()

    case problems do
      [] ->
        :ok

      _ ->
        raise ArgumentError, """
        Invalid :chat_engine configuration:

        #{Enum.map_join(problems, "\n", &("  • " <> &1))}

        Wire adapters and limits in your body's config, e.g.:

            config :chat_engine,
              persistence_adapter: MyApp.Persistence,
              conversation_store_adapter: MyApp.Conversations,
              # …
        """
    end
  end

  @doc "True if all required ports are configured and implement their behaviour. Never raises."
  @spec valid?() :: boolean()
  def valid? do
    Enum.all?(@ports, fn port -> port_problems(port) == [] end) and
      Enum.all?(@positive_int_keys, fn key -> int_problems(key) == [] end) and
      fence_problems() == []
  end

  # Defense-in-depth (B17): when `:require_fence` is set, refuse to boot on a
  # persistence adapter that does NOT export the CP fence `append/3`. Split-brain
  # protection is otherwise a silent fallback to unprotected `append/2`; a body
  # that intends to run clustered should turn this on so the guarantee is
  # boot-enforced, not documentation-enforced.
  defp fence_problems do
    if Application.get_env(:chat_engine, :require_fence, false) do
      mod = Application.get_env(:chat_engine, :persistence_adapter)

      if is_atom(mod) and mod != nil and Code.ensure_loaded?(mod) and
           not function_exported?(mod, :append, 3) do
        [
          "#{inspect(mod)} does not export the CP fence append/3, but :require_fence is set " <>
            "(clustered split-brain protection would silently fall back to unprotected append/2)"
        ]
      else
        []
      end
    else
      []
    end
  end

  # ── Per-port checks ───────────────────────────────────────────────────────────

  defp port_problems({key, behaviour, required?}) do
    case Application.get_env(:chat_engine, key) do
      nil ->
        if required?,
          do: ["#{inspect(key)} is not configured (required, implements #{inspect(behaviour)})"],
          else: []

      mod when is_atom(mod) ->
        adapter_problems(key, mod, behaviour)

      other ->
        ["#{inspect(key)} must be a module, got: #{inspect(other)}"]
    end
  end

  defp adapter_problems(key, mod, behaviour) do
    if Code.ensure_loaded?(mod) do
      missing =
        behaviour
        |> required_callbacks()
        |> Enum.reject(fn {fun, arity} -> function_exported?(mod, fun, arity) end)

      case missing do
        [] ->
          []

        _ ->
          [
            "#{inspect(key)} (#{inspect(mod)}) does not implement #{inspect(behaviour)}: missing #{format_callbacks(missing)}"
          ]
      end
    else
      [
        "#{inspect(key)} (#{inspect(mod)}) cannot be loaded — is the module compiled and on the path?"
      ]
    end
  end

  # A behaviour's required callbacks = all declared callbacks minus the optional ones.
  defp required_callbacks(behaviour) do
    behaviour.behaviour_info(:callbacks) -- behaviour.behaviour_info(:optional_callbacks)
  end

  defp format_callbacks(callbacks),
    do: Enum.map_join(callbacks, ", ", fn {f, a} -> "#{f}/#{a}" end)

  # ── Numeric knob checks ───────────────────────────────────────────────────────

  defp int_problems(key) do
    case Application.get_env(:chat_engine, key) do
      nil -> []
      n when is_integer(n) and n > 0 -> []
      other -> ["#{inspect(key)} must be a positive integer, got: #{inspect(other)}"]
    end
  end
end
