defmodule Chat.Transport do
  @moduledoc """
  Behaviour every edge/transport adapter implements so the core can push frames
  to a client without knowing whether it is a WebSocket, a gRPC stream, an in-VM
  test process, etc.

  A `Chat.Session` holds a `{module, ref}` and calls `module.push(ref, envelope)`
  to send an outbound frame. This is the seam that keeps the core
  transport-agnostic (plan Part 3).
  """
  @callback push(ref :: term(), envelope :: Chat.Envelope.t()) :: :ok
  @callback close(ref :: term(), reason :: term()) :: :ok

  @optional_callbacks close: 2
end
