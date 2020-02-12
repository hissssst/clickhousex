defmodule Clickhousex.HTTPClient do
  alias Clickhousex.Query
  @moduledoc false

  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, request, base_address, timeout, nil, _password, database) do
    opts = [timeout: timeout, recv_timeout: timeout]
    send_p(query, request, base_address, database, opts)
  end

  def send(query, request, base_address, timeout, username, password, database) do
    opts = [
      hackney: [basic_auth: {username, password}],
      timeout: timeout,
      recv_timeout: timeout
    ]
    send_p(query, request, base_address, database, opts)
  end

  defp send_p(
    %{codec: codec, type: type, external_data: external_data} = query,
    %{post_data: post_data, query_string_data: query_string_data},
    base_address,
    database,
    opts
  ) do
    command = parse_command(query)

    post_data =
      case type do
        :select ->
          query_part = query_part([post_data, " FORMAT ", codec.response_format()])
          {:multipart, [query_part | external_data_parts(external_data)]}

        _ ->
          post_data
      end

    params = %{
      database: database,
      query: IO.iodata_to_binary(query_string_data)
    }

    http_opts = Keyword.put(opts, :params, external_data_params(params, external_data))

    with(
      {:ok, %{status_code: 200, body: body}} <- HTTPoison.post(
        base_address,
        post_data,
        @req_headers, 
        http_opts
      ),
      {:command, :selected} <- {:command, command},
      {:ok, %{column_names: column_names, rows: rows}} <- codec.decode(body)
    ) do
      {:selected, column_names, rows}
    else
      {:command, :updated} ->
        {:updated, 1}

      {:ok, response} ->
        {:error, response.body}

      {:error, error} ->
        {:error, error.reason}
    end
  end

  defp parse_command(%Query{type: :select}), do: :selected
  defp parse_command(_), do: :updated

  defp query_part(query) do
    {"query", IO.iodata_to_binary(query), {"form-data", [name: "query"]}, []}
  end

  defp external_data_parts(data) do
    Enum.map(data, fn %{name: name, data: data} ->
      {name, data, {"form-data", [name: name, filename: name]}, []}
    end)
  end

  defp external_data_params(params, []), do: params
  defp external_data_params(params, data) do
    Enum.reduce(data, params, fn %{name: n, format: f, structure: s}, params ->
      Map.merge(params, %{"#{n}_structure" => s, "#{n}_format" => f})
    end)
  end
end
