defmodule Explorer.Market.History.Cataloger do
  @moduledoc """
  Fetches the daily market history.

  Market grabs the last 365 day's worth of market history for the configured
  coin in the explorer. Once that data is fetched, current day's values are
  checked every 60 minutes. Additionally, failed requests to the history
  source will follow exponential backoff `100ms * 2^(n+1)` where `n` is the
  number of failed requests.

  ## Configuration

  The following example shows the configurable values in a sample config.

      config :explorer, Explorer.Market.History.Cataloger,
        # fetch interval in milliseconds
        history_fetch_interval: :timer.minutes(60),
        # Base backoff in milliseconds for failed requests to history API
        base_backoff: 100

  """

  use GenServer

  require Logger

  alias Explorer.Market

  @typep milliseconds :: non_neg_integer()

  @impl GenServer
  def init(:ok) do
    send(self(), {:fetch_history, 365})

    {:ok, %{}}
  end

  @impl GenServer
  def handle_info({:fetch_history, day_count}, state) do
    fetch_history(day_count)

    {:noreply, state}
  end

  @impl GenServer
  # Record fetch successful.
  def handle_info({_, _, {:ok, records}}, state) do
    Market.bulk_insert_history(records)

    # Schedule next check for history
    fetch_after = config_or_default(:history_fetch_interval, :timer.minutes(60))
    Process.send_after(self(), {:fetch_history, 1}, fetch_after)

    {:noreply, state}
  end

  # Failed to get records. Try again.
  @impl GenServer
  def handle_info({day_count, failed_attempts, :error}, state) do
    Logger.warn(fn -> "Failed to fetch market history. Trying again." end)

    fetch_history(day_count, failed_attempts + 1)

    {:noreply, state}
  end

  # Callback that a monitored process has shutdown.
  @impl GenServer
  def handle_info({:DOWN, _, :process, _, _}, state) do
    {:noreply, state}
  end

  @doc """
  Starts a process to continually fetch market history.
  """
  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @spec base_backoff :: milliseconds()
  defp base_backoff do
    config_or_default(:base_backoff, 100)
  end

  @spec config_or_default(atom(), term()) :: term()
  defp config_or_default(key, default) do
    Application.get_env(:explorer, __MODULE__, [])[key] || default
  end

  @spec source_price() :: module()
  defp source_price do
    config_or_default(:source_price, Explorer.Market.History.Source.Price.CryptoCompare)
  end

  @spec source_market_cap() :: module()
  defp source_market_cap do
    config_or_default(:source_market_cap, Explorer.Market.History.Source.MarketCap.CoinGecko)
  end

  @spec fetch_history(non_neg_integer(), non_neg_integer()) :: {non_neg_integer(), non_neg_integer(), {:ok, [any()]}}
  defp fetch_history(day_count, failed_attempts \\ 0) do
    task_price =
      Task.Supervisor.async_nolink(Explorer.MarketTaskSupervisor, fn ->
        Process.sleep(delay(failed_attempts))
        source_price().fetch_history(day_count)
      end)

    task_market_cap =
      Task.Supervisor.async_nolink(Explorer.MarketTaskSupervisor, fn ->
        Process.sleep(delay(failed_attempts))
        source_market_cap().fetch_market_cap()
      end)

    tasks = [task_price, task_market_cap]

    results =
      case Task.await_many(tasks) do
        [{:ok, results_price}, {:ok, results_market_cap}] ->
          today_index =
            Enum.find_index(results_price, fn price ->
              price.date == results_market_cap.date
            end)

          today =
            results_price
            |> Enum.at(today_index)
            |> Map.put(:market_cap, results_market_cap.market_cap)

          results_price
          |> List.replace_at(today_index, today)

        [{:ok, results_price}, :error] ->
          results_price

        [:error, {:ok, results_market_cap}] ->
          results_market_cap

        _ ->
          []
      end

    {day_count, failed_attempts, {:ok, results}}
  end

  @spec delay(non_neg_integer()) :: milliseconds()
  defp delay(0), do: 0
  defp delay(1), do: base_backoff()

  defp delay(failed_attempts) do
    # Simulates 2^n
    multiplier = Enum.reduce(2..failed_attempts, 1, fn _, acc -> 2 * acc end)
    multiplier * base_backoff()
  end
end
