# Distributed tests need real peer nodes (epmd + distribution); excluded by
# default. Run them with: mix test --include distributed
#
# The Locus adapter tests need the sibling Locus binary
# (../locus/target/release/locus); they exclude themselves when it's absent.
locus_bin =
  System.get_env("LOCUS_BIN") || Path.expand("../../locus/target/release/locus", __DIR__)

locus_exclude =
  if File.exists?(locus_bin) do
    []
  else
    # Do NOT silently skip the load-bearing durability/idempotency/gap-free/CP-fence
    # proof. Make the exclusion LOUD so a green run can't be mistaken for one that
    # exercised the real adapter. CI must build/fetch Locus and set LOCUS_REQUIRED=1
    # so a missing binary FAILS the run instead of skipping it (test gap #1).
    if System.get_env("LOCUS_REQUIRED") in ["1", "true"] do
      IO.puts(
        :stderr,
        "\n\e[31mFATAL: LOCUS_REQUIRED set but the Locus binary is missing at #{locus_bin}.\e[0m"
      )

      IO.puts(
        :stderr,
        "The :locus contract/fence suite would be silently skipped. Build Locus first.\n"
      )

      System.halt(1)
    end

    IO.puts(
      :stderr,
      "\n\e[33m⚠ Locus binary not found at #{locus_bin} — EXCLUDING the :locus suite"
    )

    IO.puts(
      :stderr,
      "  (real-adapter durability / idempotency / gap-free / CP-fence proof is NOT running)."
    )

    IO.puts(
      :stderr,
      "  Build it (cd ../locus && cargo build --release) or set LOCUS_REQUIRED=1 to gate on it.\e[0m\n"
    )

    [:locus]
  end

ExUnit.start(exclude: [:distributed] ++ locus_exclude)

# Start the bundled in-memory reference adapters for the whole test run. The core
# (e.g. Chat.Presence) calls the configured adapters, so a *body* must start them;
# for the engine's own tests this test helper plays that role. (In the umbrella
# these were started incidentally by Pulsar's application — extraction makes the
# engine test suite self-contained.)
Chat.Adapters.InMemory.start_all()
