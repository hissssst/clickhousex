defmodule Clickhousex.Codec.Values do
  alias Clickhousex.Query

  def encode(%Query{param_count: 0, type: :insert}, _, []) do
    # An insert query's arguments go into the post body and the query part goes into the query string.
    # If we don't have any arguments, we don't have to encode anything, but we don't want to return
    # anything here because we'll duplicate the query into both the query string and post body
    ""
  end

  def encode(%Query{param_count: 0, statement: statement}, _, []) do
    statement
  end
  def encode(%Query{param_count: 0}, _, _) do
    raise ArgumentError, "Extra params! Query doesn't contain '?'"
  end
  def encode(%Query{param_count: param_count} = query, query_text, params) do
    query_parts = String.split(query_text, "?")
    weave(query, query_parts, params, param_count)
  end

  defp weave(query, [part | parts], [param | params], param_count) do
    [part, encode_param(query, param) | weave(query, parts, params, param_count - 1)]
  end
  defp weave(_query, [_] = parts, [], 1), do: parts
  defp weave(_query, [_], [], _) do
    raise ArgumentError,
      "The number of parameters does not correspond to the number of question marks!"
  end

  defp encode_param(query, param) when is_list(param) do
    values = Enum.map_join(param, ",", &encode_param(query, &1))

    case query do
      # We pass lists to in clauses, and they shouldn't have brackets around them.
      %{type: :select} -> values
      _ -> "[#{values}]"
    end
  end
  defp encode_param(_query, param) when is_integer(param) do
    Integer.to_string(param)
  end
  defp encode_param(_query, true), do: "1"
  defp encode_param(_query, false), do: "0"
  defp encode_param(_query, param) when is_float(param) do
    to_string(param)
  end
  defp encode_param(_query, nil), do: "NULL"
  defp encode_param(%{codec: Clickhousex.Codec.RowBinary}, %DateTime{} = dt) do
    DateTime.to_unix(dt)
  end
  defp encode_param(_query, %DateTime{} = datetime) do
    iso_date = DateTime.truncate(datetime, :second)
    "'#{Date.to_string iso_date} #{Time.to_string iso_date}'"
  end
  defp encode_param(_query, %NaiveDateTime{} = naive_datetime) do
    naive = DateTime.truncate(naive_datetime, :second)
    "'#{Date.to_string naive} #{Time.to_string naive}'"
  end
  defp encode_param(_query, %Date{} = date), do: "'#{date}'"
  defp encode_param(_query, param) do
    "'#{escape param}'"
  end

  defp escape(s) do
    s
    |> String.replace("_", "\_")
    |> String.replace("'", "\'")
    |> String.replace("%", "\%")
    |> String.replace(~s("), ~s(\\"))
    |> String.replace("\\", "\\\\")
  end
end
