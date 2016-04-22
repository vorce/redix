defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.Utils
  alias Redix.Connection.Receiver

  require Logger

  @type state :: %__MODULE__{}

  defstruct [
    # The TCP socket that holds the connection to Redis
    socket: nil,
    # Options passed when the connection is started
    opts: nil,
    # The receiver process
    receiver: nil,
    # TODO: document this
    current_backoff: nil,
    socket_checked_out?: false,
    checkout_queue: :queue.new,
  ]

  @initial_backoff 500

  @backoff_exponent 1.5

  ## Functions executed with the checked out socket

  def pipeline(conn, commands, opts) do
    timeout = opts[:timeout] || 5_000
    ncommands = length(commands)

    case Connection.call(conn, :checkout_socket, timeout) do
      {:ok, socket} ->
        case :gen_tcp.send(socket, Enum.map(commands, &Protocol.pack/1)) do
          :ok ->
            resp = recv(socket, ncommands, timeout, nil)
            Connection.call(conn, :checkin_socket)
            resp
          {:error, _reason} = error ->
            Connection.cast(conn, {:disconnect, error})
            error
        end
      {:error, _reason} = error ->
        error
    end
  end

  defp recv(socket, ncommands, timeout, cont) do
    parser = cont || &Redix.Protocol.parse_multi(&1, ncommands)
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, data} ->
        case parser.(data) do
          {:ok, resp, rest} ->
            if byte_size(rest) > 0 do
              Logger.error "there's a rest: #{inspect rest}"
            end
            format_resp(resp)
          {:continuation, cont} ->
            recv(socket, ncommands, timeout, cont)
        end
      {:error, _reason} = error ->
        error
    end
  end

  ## Callbacks

  @doc false
  def init(opts) do
    state = %__MODULE__{opts: opts}

    if opts[:sync_connect] do
      sync_connect(state)
    else
      {:connect, :init, state}
    end
  end

  @doc false
  def connect(info, state)

  def connect(info, state) do
    case Utils.connect(state) do
      {:ok, state} ->
        {:ok, state}
      {:error, reason} ->
        Logger.error [
          "Failed to connect to Redis (", Utils.format_host(state), "): ",
          Utils.format_error(reason),
        ]

        if info == :init do
          {:backoff, @initial_backoff, %{state | current_backoff: @initial_backoff}}
        else
          max_backoff = state.opts[:max_backoff]
          next_exponential_backoff = round(state.current_backoff * @backoff_exponent)
          next_backoff =
            if max_backoff == :infinity do
              next_exponential_backoff
            else
              min(next_exponential_backoff, max_backoff)
            end
          {:backoff, next_backoff, %{state | current_backoff: next_backoff}}
        end
      other ->
        other
    end
  end

  @doc false
  def disconnect(reason, state)

  def disconnect(:stop, state) do
    {:stop, :normal, state}
  end

  def disconnect({:error, reason} = _error, state) do
    Logger.error ["Disconnected from Redis (#{Utils.format_host(state)}): ",
                  Utils.format_error(reason)]

    :gen_tcp.close(state.socket)

    {:backoff, @initial_backoff, %{reset_state(state) | current_backoff: @initial_backoff}}
  end

  @doc false
  def handle_call(operation, from, state)

  def handle_call(:checkout_socket, _from, %{socket: nil} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call(:checkout_socket, from, %{socket_checked_out?: true} = state) do
    {:noreply, %{state | checkout_queue: :queue.in(from, state.checkout_queue)}}
  end

  def handle_call(:checkout_socket, _from, state) do
    # Let's deactivate the socket.
    :ok = :inet.setopts(state.socket, active: false)
    state = %{state | socket_checked_out?: true}
    {:reply, {:ok, state.socket}, state}
  end

  def handle_call(:checkin_socket, _from, state) do
    case :queue.out(state.checkout_queue) do
      {{:value, from}, new_queue} ->
        GenServer.reply(from, {:ok, state.socket})
        {:reply, :ok, %{state | checkout_queue: new_queue}}
      {:empty, _} ->
        :ok = :inet.setopts(state.socket, active: :once)
        {:reply, :ok, %{state | socket_checked_out?: false}}
    end
  end

  @doc false
  def handle_cast(operation, state)

  def handle_cast(:stop, state) do
    {:disconnect, :stop, state}
  end

  def handle_cast({:disconnect, reason}, state) do
    {:disconnect, reason, state}
  end

  @doc false
  def handle_info(msg, state)

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :tcp_closed}, state}
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  ## Helper functions

  defp sync_connect(state) do
    case Utils.connect(state) do
      {:ok, _state} = result ->
        result
      {:error, reason} ->
        {:stop, reason}
      {:stop, reason, _state} ->
        {:stop, reason}
    end
  end

  defp reset_state(state) do
    %{state | socket: nil}
  end

  defp format_resp(%Redix.Error{} = err), do: {:error, err}
  defp format_resp(resp), do: {:ok, resp}
end
