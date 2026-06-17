# Distributed tests need real peer nodes (epmd + distribution); excluded by
# default. Run them with: mix test --include distributed
ExUnit.start(exclude: [:distributed])

# Start the bundled in-memory reference adapters for the whole test run. The core
# (e.g. Chat.Presence) calls the configured adapters, so a *body* must start them;
# for the engine's own tests this test helper plays that role. (In the umbrella
# these were started incidentally by Pulsar's application — extraction makes the
# engine test suite self-contained.)
Chat.Adapters.InMemory.start_all()
