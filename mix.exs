defmodule Chat.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/intenttext/chat-engine"

  def project do
    [
      app: :chat_engine,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "chat_engine",
      source_url: @source_url,
      docs: [main: "readme", extras: ["README.md"]]
    ]
  end

  # The OTP application. `mod:` boots `Chat.Application` (registries + the
  # conversation/session supervisors). Concrete adapters are started by the
  # *body*, not here.
  #
  # FIREWALL: this app lists NO transport/DB/web deps — only `:syn` (cluster
  # registry). `Chat.FirewallTest` enforces it. The bundled in-memory reference
  # adapters use only stdlib, so they don't breach it.
  def application do
    [
      extra_applications: [:logger],
      mod: {Chat.Application, []}
    ]
  end

  defp deps do
    [
      {:syn, "~> 3.3"},
      {:stream_data, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp description do
    "A real-time messaging engine core — ordering, fan-out, presence, receipts, " <>
      "offline catch-up, and clustering behind clean ports. Ships no database and no transport; " <>
      "you supply those through small behaviours. Includes in-memory reference adapters."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib config mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end
end
