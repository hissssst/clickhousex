defmodule Clickhousex.Codec.JSON do
  @behaviour Clickhousex.Codec

  defdelegate encode(query, replacements, params), to: Clickhousex.Codec.Values

  @impl Clickhousex.Codec
  def request_format(), do: "Values"

  @impl Clickhousex.Codec
  def response_format(), do: "JSONCompact"

  @impl Clickhousex.Codec
  def decode(response) do
    %{
      "meta" => meta,
      "data" => data,
      "rows" => row_count
    } = Jason.decode!(response)

    column_names = Enum.map(meta, &Map.get(&1, "name"))
    column_types = Enum.map(meta, &Map.get(&1, "type"))

    rows =
      Enum.map(data, fn row ->
        row
        |> Enum.zip(column_types)
        |> Enum.map(fn {raw_value, column_type} ->
          to_native(column_type, raw_value)
        end)
        |> List.to_tuple()
      end)

    {:ok, %{column_names: column_names, rows: rows, count: row_count}}
  end

  defp to_native(_, nil), do: nil
  defp to_native(<<"Nullable(", type::binary>>, value) do
    type = String.trim_trailing(type, ")")
    to_native(type, value)
  end
  defp to_native(<<"Array(", type::binary>>, value) do
    type = String.trim_trailing(type, ")")
    Enum.map(value, &to_native(type, &1))
  end
  defp to_native("Float" <> _, value) when is_integer(value) do
    1.0 * value
  end
  defp to_native("Int64", value) do
    String.to_integer(value)
  end
  defp to_native("Date", value) do
    Date.from_iso8601!(value)
  end
  defp to_native("DateTime", value) do
    NaiveDateTime.from_iso8601!(value)
  end
  defp to_native("UInt" <> _, value) when is_bitstring(value) do
    String.to_integer(value)
  end
  defp to_native("Int" <> _, value) when is_bitstring(value) do
    String.to_integer(value)
  end
  defp to_native(_, value) do
    value
  end
end
