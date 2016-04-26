defmodule Redix.Connection do
  @moduledoc false

  use DBConnection

  defstruct sock: nil, continuation: nil, tail: ""

  defmodule Query do
    defstruct [:query]
  end

  defmodule Error do
    defexception [:function, :reason, :message]

    def exception({function, reason}) do
      message = "#{function} error: #{format_error(reason)}"
      %Error{function: function, reason: reason, message: message}
    end

    defp format_error(error), do: inspect(error)
  end

  def pipeline(conn, commands, timeout) do
    DBConnection.query(conn, %Query{query: :pipeline}, {commands, timeout}, pool: DBConnection.Poolboy)
  end

  ## Callbacks

  def connect(opts) do
    host        = Keyword.get(opts, :host, "localhost") |> String.to_char_list()
    port        = Keyword.get(opts, :port, 6379)
    socket_opts = Keyword.get(opts, :socket_options, [])
    timeout     = Keyword.get(opts, :connect_timeout, 5_000)

    enforced_opts = [packet: :raw, mode: :binary, active: :once]
    # :gen_tcp.connect gives priority to options at tail, rather than head.
    socket_opts = Enum.reverse(socket_opts, enforced_opts)
    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, sock} ->
        {:ok, %__MODULE__{sock: sock}}
      {:error, reason} ->
        {:error, Error.exception({:connect, reason})}
    end
  end

  def checkout(state) do
    case :inet.setopts(state.sock, active: false) do
      :ok ->
        flush(state)
      {:error, reason} ->
        {:disconnect, Error.exception({:setopts, reason}), state}
    end
  end

  def checkin(state) do
    case :inet.setopts(state.sock, active: :once) do
      :ok ->
        {:ok, state}
      {:error, reason} ->
        {:disconnect, Error.exception({:setopts, reason}), state}
    end
  end

  def handle_execute(%Query{query: :pipeline} = query, {commands, timeout}, _opts, state) do
    data = Enum.map(commands, &Redix.Protocol.pack/1)
    case :gen_tcp.send(state.sock, data) do
      :ok ->
        recv(length(commands), timeout, state)
      {:error, reason} ->
        {:disconnect, Error.exception({:send, reason}), state}
    end
  end

  # No tail means we always try to recv new data from the socket.
  def recv(ncommands, timeout, %{tail: ""} = state) do
    parser = state.continuation || &Redix.Protocol.parse_multi(&1, ncommands)
    case :gen_tcp.recv(state.sock, 0, timeout) do
      {:ok, data} ->
        case parser.(data) do
          {:ok, resp, tail} ->
            {:ok, resp, %{state | tail: tail}}
          {:continuation, cont} ->
            recv(ncommands, timeout, %{state | continuation: cont})
        end
      {:error, reason} ->
        {:disconnect, Error.exception({:recv, reason}), state}
    end
  end

  # If the tail is not empty, we first try to parse the tail, otherwise we recv
  # recursively.
  def recv(ncommands, timeout, state) do
    parser = state.continuation || &Redix.Protocol.parse_multi(&1, ncommands)
    case parser.(state.tail) do
      {:ok, resp, tail} ->
        {:ok, resp, %{state | tail: tail}}
      {:continuation, cont} ->
        recv(ncommands, timeout, %{state | tail: "", continuation: cont})
    end
  end

  def handle_close(_, _, s) do
    {:ok, nil, s}
  end

  def handle_info({:tcp_closed, sock}, %{sock: sock} = state) do
    {:disconnect, Error.exception({:recv, :closed}), state}
  end

  def handle_info({:tcp_error, sock, reason}, %{sock: sock} = state) do
    {:disconnect, Error.exception({:recv, reason}), state}
  end

  def handle_info(_, state), do: {:ok, state}

  def disconnect(_, state) do
    :ok = :gen_tcp.close(state.sock)
    _ = flush(state)
    :ok
  end

  ## Helpers

  defp flush(%{sock: sock} = state) do
    receive do
      {:tcp, ^sock, data} ->
        {:ok, %{state | tail: state.tail <> data}}
      {:tcp_closed, ^sock} ->
        {:disconnect, Error.exception({:recv, :closed}), state}
      {:tcp_error, ^sock, reason} ->
        {:disconnect, Error.exception({:recv, reason}), state}
    after
      0 -> {:ok, state}
    end
  end
end

defimpl DBConnection.Query, for: Redix.Connection.Query do
  alias Redix.Connection.Query

  def parse(%Query{} = query, _), do: query
  def describe(%Query{} = query, _), do: query
  def encode(%Query{}, args, _), do: args
  def decode(_, result, _), do: result
end
