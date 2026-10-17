defmodule Natsbeam.Protocol.Parser do
  @moduledoc """
  Incremental parser for the client side of the NATS wire protocol.

  Feed it bytes as they arrive with `parse/2`; it returns a list of parsed
  operations and the updated parser state. Payload-carrying operations
  (`PUB`/`HPUB`) span a control line plus a payload terminated by CRLF; the
  parser keeps whatever it still needs across calls.

  `PUB`/`HPUB`/`PING`/`PONG` in client mode take a fast path: the control
  line is scanned byte by byte with binary pattern matching (no
  `String`/`:binary` calls, no intermediate lists) and the payload is
  matched straight out of the buffer. Anything the fast path does not
  recognise with certainty (other verbs, lowercase verbs, tabs, repeated
  spaces, bare `\\n` line endings, odd sizes) is handed to the generic
  line-oriented path, which also serves the route/leaf/gateway modes.
  """

  defstruct buf: "",
            pending: nil,
            max_control_line: 4096,
            max_payload: 1_048_576,
            mode: :client,
            route_account: nil

  @type op ::
          {:connect, map()}
          | {:pub, subject :: binary(), reply :: binary() | nil, payload :: binary()}
          | {:hpub, subject :: binary(), reply :: binary() | nil, headers :: binary(),
             payload :: binary()}
          | {:sub, subject :: binary(), queue :: binary() | nil, sid :: binary()}
          | {:unsub, sid :: binary(), max :: non_neg_integer() | nil}
          | :ping
          | :pong
          | {:info, map()}
          | {:unknown, binary()}

  @type t :: %__MODULE__{}

  @min_hdr byte_size("NATS/1.0\r\n\r\n")

  def new(opts \\ []) do
    %__MODULE__{
      max_control_line: Keyword.get(opts, :max_control_line, 4096),
      max_payload: Keyword.get(opts, :max_payload, 1_048_576),
      # :client, or :route / :leaf for server-to-server links (RS+/RS-/RMSG/HMSG/LS+/LS-/LMSG),
      # or :gateway (route ops plus A+/A-)
      mode: Keyword.get(opts, :mode, :client),
      # Pinned-account route connections (nats-server `route_account`) omit the
      # account from RS+/RS-/RMSG/HMSG; this is the account those frames carry.
      route_account: Keyword.get(opts, :route_account)
    }
  end

  @doc """
  Set (or clear) the pinned account of a route connection. Route-mode parsing
  stops after every `INFO`, so a link can switch this before the frames that
  follow the peer's INFO are parsed (resume with `parse(parser, "")`).
  """
  def set_route_account(%__MODULE__{} = p, account), do: %{p | route_account: account}

  @doc """
  Parse newly received `data`. Returns `{:ok, ops, parser}` or
  `{:error, reason, parser}` where reason is a NATS-style error string.
  """
  @spec parse(t(), binary()) :: {:ok, [op()], t()} | {:error, binary(), t()}
  def parse(%__MODULE__{buf: "", pending: nil} = p, data), do: do_parse(p, data, [])
  def parse(%__MODULE__{buf: "", pending: pending} = p, data), do: payload(p, pending, data, [])
  def parse(%__MODULE__{buf: buf, pending: nil} = p, data), do: do_parse(p, buf <> data, [])

  def parse(%__MODULE__{buf: buf, pending: pending} = p, data),
    do: payload(p, pending, buf <> data, [])

  # The parser struct is only rebuilt when a call returns; in between, the
  # buffer travels as a plain argument and `pending` is always nil.

  # ------------------------------------------------------------- fast path

  defp do_parse(%{mode: :client} = p, buf, acc) do
    case buf do
      <<"PUB ", rest::binary>> -> pub(p, buf, rest, acc)
      <<"HPUB ", rest::binary>> -> hpub(p, buf, rest, acc)
      <<"PING\r\n", rest::binary>> -> do_parse(p, rest, [:ping | acc])
      <<"PONG\r\n", rest::binary>> -> do_parse(p, rest, [:pong | acc])
      _ -> generic(p, buf, acc)
    end
  end

  defp do_parse(p, buf, acc), do: generic(p, buf, acc)

  # PUB <subject> [reply] <size>\r\n<payload>\r\n
  defp pub(p, buf, rest, acc) do
    max = p.max_control_line

    case token(rest, 0, max) do
      {:space, slen} when slen > 0 ->
        <<subject::binary-size(slen), " ", r1::binary>> = rest

        case token(r1, 0, max) do
          {:crlf, nlen} when nlen > 0 ->
            <<size::binary-size(nlen), "\r\n", r2::binary>> = r1
            pub_size(p, buf, subject, nil, size, 5 + slen + nlen, r2, acc)

          {:space, rlen} when rlen > 0 ->
            <<reply::binary-size(rlen), " ", r2::binary>> = r1

            case token(r2, 0, max) do
              {:crlf, nlen} when nlen > 0 ->
                <<size::binary-size(nlen), "\r\n", r3::binary>> = r2
                pub_size(p, buf, subject, reply, size, 6 + slen + rlen + nlen, r3, acc)

              _ ->
                generic(p, buf, acc)
            end

          _ ->
            generic(p, buf, acc)
        end

      _ ->
        generic(p, buf, acc)
    end
  end

  defp pub_size(p, buf, subject, reply, size, line_len, rest, acc) do
    if line_len > p.max_control_line do
      generic(p, buf, acc)
    else
      case digits(size, 0) do
        :error ->
          generic(p, buf, acc)

        n when n > p.max_payload ->
          {:error, "Maximum Payload Violation", %{p | buf: rest, pending: nil}}

        n ->
          case rest do
            <<body::binary-size(n), "\r\n", r::binary>> ->
              do_parse(p, r, [{:pub, subject, reply, body} | acc])

            _ ->
              payload(p, {:pub, subject, reply, n}, rest, acc)
          end
      end
    end
  end

  # HPUB <subject> [reply] <hdr_len> <total_len>\r\n<headers><payload>\r\n
  defp hpub(p, buf, rest, acc) do
    max = p.max_control_line

    case token(rest, 0, max) do
      {:space, slen} when slen > 0 ->
        <<subject::binary-size(slen), " ", r1::binary>> = rest

        case token(r1, 0, max) do
          {:space, l2} when l2 > 0 ->
            <<t2::binary-size(l2), " ", r2::binary>> = r1

            case token(r2, 0, max) do
              {:crlf, l3} when l3 > 0 ->
                <<t3::binary-size(l3), "\r\n", r3::binary>> = r2
                hpub_sizes(p, buf, subject, nil, t2, t3, 7 + slen + l2 + l3, r3, acc)

              {:space, l3} when l3 > 0 ->
                <<t3::binary-size(l3), " ", r3::binary>> = r2

                case token(r3, 0, max) do
                  {:crlf, l4} when l4 > 0 ->
                    <<t4::binary-size(l4), "\r\n", r4::binary>> = r3
                    hpub_sizes(p, buf, subject, t2, t3, t4, 8 + slen + l2 + l3 + l4, r4, acc)

                  _ ->
                    generic(p, buf, acc)
                end

              _ ->
                generic(p, buf, acc)
            end

          _ ->
            generic(p, buf, acc)
        end

      _ ->
        generic(p, buf, acc)
    end
  end

  defp hpub_sizes(p, buf, subject, reply, hs, ts, line_len, rest, acc) do
    if line_len > p.max_control_line do
      generic(p, buf, acc)
    else
      case {digits(hs, 0), digits(ts, 0)} do
        {h, t} when is_integer(h) and is_integer(t) ->
          cond do
            t > p.max_payload ->
              {:error, "Maximum Payload Violation", %{p | buf: rest, pending: nil}}

            h > t or h < @min_hdr ->
              {:error, "Parser Error", %{p | buf: rest, pending: nil}}

            true ->
              case rest do
                <<headers::binary-size(h), body::binary-size(t - h), "\r\n", r::binary>> ->
                  if valid_headers?(headers) do
                    do_parse(p, r, [{:hpub, subject, reply, headers, body} | acc])
                  else
                    {:error, "Parser Error", %{p | buf: r, pending: nil}}
                  end

                _ ->
                  payload(p, {:hpub, subject, reply, h, t}, rest, acc)
              end
          end

        _ ->
          generic(p, buf, acc)
      end
    end
  end

  # Scan one control-line token. Returns `{:space, len}` or `{:crlf, len}`
  # for a token ended by a single space or CRLF, or `:fallback` on anything
  # the fast path should not decide (tab, bare CR/LF, end of buffer, or a
  # token longer than the control line limit).
  defp token(<<" ", _::binary>>, n, _max), do: {:space, n}
  defp token(<<"\r\n", _::binary>>, n, _max), do: {:crlf, n}

  defp token(<<c, _::binary>>, _n, _max) when c == ?\t or c == ?\r or c == ?\n,
    do: :fallback

  defp token(<<_, rest::binary>>, n, max) when n < max, do: token(rest, n + 1, max)
  defp token(_, _n, _max), do: :fallback

  defp digits(<<d, rest::binary>>, acc) when d >= ?0 and d <= ?9,
    do: digits(rest, acc * 10 + (d - ?0))

  defp digits(<<>>, acc), do: acc
  defp digits(_, _), do: :error

  # --------------------------------------------------------------- payload

  # Waiting for (the rest of) the payload of a PUB/HPUB/RMSG/LMSG. The total
  # payload length is always the last element of the pending tuple.
  defp payload(p, pending, buf, acc) do
    total = :erlang.element(tuple_size(pending), pending)

    case buf do
      <<body::binary-size(total), "\r\n", rest::binary>> ->
        case complete(pending, body) do
          {:ok, op} -> do_parse(p, rest, [op | acc])
          {:error, reason} -> {:error, reason, %{p | buf: rest, pending: nil}}
        end

      <<_::binary-size(total), _, _, _::binary>> ->
        {:error, "Parser Error", %{p | buf: "", pending: nil}}

      _ ->
        {:ok, Enum.reverse(acc), %{p | buf: buf, pending: pending}}
    end
  end

  defp complete({:pub, subject, reply, _}, body), do: {:ok, {:pub, subject, reply, body}}

  defp complete({:hpub, subject, reply, h, _}, body) do
    <<headers::binary-size(h), payload::binary>> = body

    if valid_headers?(headers),
      do: {:ok, {:hpub, subject, reply, headers, payload}},
      else: {:error, "Parser Error"}
  end

  defp complete({:rmsg, acc, subject, reply, queues, nil, _}, body),
    do: {:ok, {:rmsg, acc, subject, reply, queues, nil, body}}

  defp complete({:rmsg, acc, subject, reply, queues, h, _}, body) do
    <<headers::binary-size(h), payload::binary>> = body
    {:ok, {:rmsg, acc, subject, reply, queues, if(h > 0, do: headers), payload}}
  end

  defp complete({:lmsg, subject, reply, queues, nil, _}, body),
    do: {:ok, {:lmsg, subject, reply, queues, nil, body}}

  defp complete({:lmsg, subject, reply, queues, h, _}, body) do
    <<headers::binary-size(h), payload::binary>> = body
    {:ok, {:lmsg, subject, reply, queues, if(h > 0, do: headers), payload}}
  end

  defp valid_headers?(<<"NATS/1.0", _::binary>> = headers) when byte_size(headers) >= 4,
    do: binary_part(headers, byte_size(headers) - 4, 4) == "\r\n\r\n"

  defp valid_headers?(_), do: false

  # ---------------------------------------------------------- generic path

  defp generic(p, buf, acc) do
    case :binary.match(buf, "\r\n") do
      :nomatch ->
        # Also tolerate bare "\n" lines? nats-server requires CRLF but accepts
        # LF-terminated control lines in practice (it scans for '\n'). We mirror it.
        case :binary.match(buf, "\n") do
          :nomatch ->
            if byte_size(buf) > p.max_control_line do
              {:error, "Maximum Control Line Exceeded", %{p | buf: "", pending: nil}}
            else
              {:ok, Enum.reverse(acc), %{p | buf: buf, pending: nil}}
            end

          {pos, 1} ->
            handle_line(p, buf, pos, 1, acc)
        end

      {pos, 2} ->
        handle_line(p, buf, pos, 2, acc)
    end
  end

  defp handle_line(p, buf, pos, sep_len, acc) do
    if pos > p.max_control_line do
      {:error, "Maximum Control Line Exceeded", %{p | buf: "", pending: nil}}
    else
      <<line::binary-size(pos), _::binary-size(sep_len), rest::binary>> = buf
      line = String.trim_trailing(line, "\r")

      case parse_line(line, p) do
        {:ok, {:info, _} = op} when p.mode == :route ->
          {:ok, Enum.reverse([op | acc]), %{p | buf: rest, pending: nil}}

        {:ok, op} ->
          do_parse(p, rest, [op | acc])

        {:pending, pending} ->
          payload(p, pending, rest, acc)

        :skip ->
          do_parse(p, rest, acc)

        {:error, reason} ->
          {:error, reason, %{p | buf: rest, pending: nil}}
      end
    end
  end

  defp parse_line("", _p), do: :skip

  defp parse_line(line, p) do
    {op, args} =
      case :binary.match(line, [" ", "\t"]) do
        :nomatch ->
          {line, ""}

        {i, _} ->
          {binary_part(line, 0, i),
           String.trim_leading(binary_part(line, i, byte_size(line) - i))}
      end

    case {String.upcase(op), p.mode} do
      {"CONNECT", _} -> parse_connect(args)
      {"PUB", :client} -> parse_pub(args, p)
      {"HPUB", :client} -> parse_hpub(args, p)
      {"SUB", :client} -> parse_sub(args)
      {"UNSUB", :client} -> parse_unsub(args)
      {"PING", _} -> {:ok, :ping}
      {"PONG", _} -> {:ok, :pong}
      {"INFO", _} -> parse_info(args)
      {"+OK", _} -> :skip
      {"-ERR", _} -> {:ok, {:err, String.trim(args, "'")}}
      {"RS+", :route} -> parse_rs_plus(route_args(args, p))
      {"RS-", :route} -> parse_rs_minus(route_args(args, p))
      {"RMSG", :route} -> parse_rmsg(route_args(args, p), p)
      {"HMSG", :route} -> parse_rhmsg(route_args(args, p), p)
      {"LS+", :leaf} -> parse_ls_plus(args)
      {"LS-", :leaf} -> parse_ls_minus(args)
      {"LMSG", :leaf} -> parse_leaf_msg(args, p)
      {"HMSG", :leaf} -> parse_lmsg(args, p)
      # Gateways: route-style RS+/RS-/RMSG/HMSG (always account-prefixed)
      # plus the account-level A+/A- interest commands.
      {"RS+", :gateway} -> parse_rs_plus(split_args(args))
      {"RS-", :gateway} -> parse_rs_minus(split_args(args))
      {"RMSG", :gateway} -> parse_rmsg(split_args(args), p)
      {"HMSG", :gateway} -> parse_rhmsg(split_args(args), p)
      {"A+", :gateway} -> parse_account_cmd(:a_plus, args)
      {"A-", :gateway} -> parse_account_cmd(:a_minus, args)
      _ -> {:error, "Unknown Protocol Operation"}
    end
  end

  # ---------------------------------------------------------------- routes
  #
  # RS+ <acc> <subject> [<queue> <weight>]
  # RS- <acc> <subject> [<queue>]
  # RMSG <acc> <subject> [<reply> | + <reply> q.. | | q..] <size>
  # HMSG <acc> <subject> [...] <hdr_len> <total_len>
  #
  # On a pinned-account connection (`route_account`) the <acc> argument is
  # absent; the pinned account is prepended so both forms parse the same way.

  defp route_args(args, %{route_account: nil}), do: split_args(args)
  defp route_args(args, %{route_account: acc}), do: [acc | split_args(args)]

  defp parse_rs_plus(args) do
    case args do
      [acc, subject] ->
        {:ok, {:rs_plus, acc, subject, nil, 1}}

      [acc, subject, queue, weight] ->
        case parse_int(weight) do
          {:ok, w} -> {:ok, {:rs_plus, acc, subject, queue, w}}
          err -> err
        end

      _ ->
        {:error, "Parser Error"}
    end
  end

  defp parse_rs_minus(args) do
    case args do
      [acc, subject] -> {:ok, {:rs_minus, acc, subject, nil}}
      [acc, subject, queue] -> {:ok, {:rs_minus, acc, subject, queue}}
      _ -> {:error, "Parser Error"}
    end
  end

  # Returns {reply, queues, rest_fields} for the middle part of RMSG/HMSG.
  defp routed_targets(fields) do
    case fields do
      ["+", reply | rest] ->
        {qs, sizes} = Enum.split(rest, -1)
        {reply, qs, sizes}

      ["|" | rest] ->
        {qs, sizes} = Enum.split(rest, -1)
        {nil, qs, sizes}

      [size] ->
        {nil, [], [size]}

      [reply, size] ->
        {reply, [], [size]}

      _ ->
        :error
    end
  end

  defp routed_targets_h(fields) do
    case fields do
      ["+", reply | rest] ->
        {qs, sizes} = Enum.split(rest, -2)
        {reply, qs, sizes}

      ["|" | rest] ->
        {qs, sizes} = Enum.split(rest, -2)
        {nil, qs, sizes}

      [h, t] ->
        {nil, [], [h, t]}

      [reply, h, t] ->
        {reply, [], [h, t]}

      _ ->
        :error
    end
  end

  defp parse_rmsg(args, p) do
    with [acc, subject | rest] <- args,
         {reply, queues, [size]} <- routed_targets(rest),
         {:ok, n} <- parse_int(size),
         :ok <- check_payload(n, p) do
      {:pending, {:rmsg, acc, subject, reply, queues, nil, n}}
    else
      {:error, _} = e -> e
      _ -> {:error, "Parser Error"}
    end
  end

  defp parse_rhmsg(args, p) do
    with [acc, subject | rest] <- args,
         {reply, queues, [hdr, total]} <- routed_targets_h(rest),
         {:ok, h} <- parse_int(hdr),
         {:ok, t} <- parse_int(total),
         :ok <- check_payload(t, p),
         true <- h <= t do
      {:pending, {:rmsg, acc, subject, reply, queues, h, t}}
    else
      {:error, _} = e -> e
      _ -> {:error, "Parser Error"}
    end
  end

  # A+ <acc> / A- <acc> (gateways)
  defp parse_account_cmd(op, args) do
    case split_args(args) do
      [acc] -> {:ok, {op, acc}}
      _ -> {:error, "Parser Error"}
    end
  end

  # ------------------------------------------------------------------ leaf
  #
  # LS+ <subject> [<queue> <weight>]
  # LS- <subject> [<queue>]
  # LMSG <subject> [<reply> | + <reply> q.. | | q..] <hdr_len> <total_len>
  # MSG  <subject> [<reply> ...] <size>            (no headers)

  defp parse_ls_plus(args) do
    case split_args(args) do
      [subject] ->
        {:ok, {:ls_plus, subject, nil, 1}}

      [subject, queue, weight] ->
        case parse_int(weight) do
          {:ok, w} -> {:ok, {:ls_plus, subject, queue, w}}
          err -> err
        end

      _ ->
        {:error, "Parser Error"}
    end
  end

  defp parse_ls_minus(args) do
    case split_args(args) do
      [subject] -> {:ok, {:ls_minus, subject, nil}}
      [subject, queue] -> {:ok, {:ls_minus, subject, queue}}
      _ -> {:error, "Parser Error"}
    end
  end

  defp parse_lmsg(args, p) do
    with [subject | rest] <- split_args(args),
         {reply, queues, [hdr, total]} <- routed_targets_h(rest),
         {:ok, h} <- parse_int(hdr),
         {:ok, t} <- parse_int(total),
         :ok <- check_payload(t, p),
         true <- h <= t do
      {:pending, {:lmsg, subject, reply, queues, h, t}}
    else
      {:error, _} = e -> e
      _ -> {:error, "Parser Error"}
    end
  end

  defp parse_leaf_msg(args, p) do
    with [subject | rest] <- split_args(args),
         {reply, queues, [size]} <- routed_targets(rest),
         {:ok, n} <- parse_int(size),
         :ok <- check_payload(n, p) do
      {:pending, {:lmsg, subject, reply, queues, nil, n}}
    else
      {:error, _} = e -> e
      _ -> {:error, "Parser Error"}
    end
  end

  defp parse_connect(args) do
    case Jason.decode(args) do
      {:ok, map} when is_map(map) -> {:ok, {:connect, map}}
      _ -> {:error, "Parser Error"}
    end
  end

  defp parse_info(args) do
    case Jason.decode(args) do
      {:ok, map} when is_map(map) -> {:ok, {:info, map}}
      _ -> {:error, "Parser Error"}
    end
  end

  defp parse_pub(args, p) do
    case split_args(args) do
      [subject, size] -> pub_pending(subject, nil, size, p)
      [subject, reply, size] -> pub_pending(subject, reply, size, p)
      _ -> {:error, "Parser Error"}
    end
  end

  defp pub_pending(subject, reply, size, p) do
    with {:ok, n} <- parse_int(size),
         :ok <- check_payload(n, p) do
      {:pending, {:pub, subject, reply, n}}
    end
  end

  defp parse_hpub(args, p) do
    case split_args(args) do
      [subject, hdr, total] -> hpub_pending(subject, nil, hdr, total, p)
      [subject, reply, hdr, total] -> hpub_pending(subject, reply, hdr, total, p)
      _ -> {:error, "Parser Error"}
    end
  end

  defp hpub_pending(subject, reply, hdr, total, p) do
    with {:ok, h} <- parse_int(hdr),
         {:ok, t} <- parse_int(total),
         :ok <- check_payload(t, p) do
      if h > t or h < @min_hdr do
        {:error, "Parser Error"}
      else
        {:pending, {:hpub, subject, reply, h, t}}
      end
    end
  end

  defp parse_sub(args) do
    case split_args(args) do
      [subject, sid] -> {:ok, {:sub, subject, nil, sid}}
      [subject, queue, sid] -> {:ok, {:sub, subject, queue, sid}}
      _ -> {:error, "Parser Error"}
    end
  end

  defp parse_unsub(args) do
    case split_args(args) do
      [sid] ->
        {:ok, {:unsub, sid, nil}}

      [sid, max] ->
        case parse_int(max) do
          {:ok, n} -> {:ok, {:unsub, sid, n}}
          err -> err
        end

      _ ->
        {:error, "Parser Error"}
    end
  end

  defp split_args(args), do: String.split(args, [" ", "\t"], trim: true)

  defp parse_int(s) do
    case Integer.parse(s) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> {:error, "Parser Error"}
    end
  end

  defp check_payload(n, %{max_payload: max}) when n > max,
    do: {:error, "Maximum Payload Violation"}

  defp check_payload(_, _), do: :ok
end
