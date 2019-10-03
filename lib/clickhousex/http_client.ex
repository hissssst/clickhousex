defmodule Clickhousex.HTTPClient do
  alias Clickhousex.Query
  @moduledoc false

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, request, base_address, timeout, nil, _password, database) do
    send_p(query, request, base_address, database, timeout: timeout, recv_timeout: timeout)
  end

  def send(query, request, base_address, timeout, username, password, database) do
    opts = [hackney: [basic_auth: {username, password}], timeout: timeout, recv_timeout: timeout]
    send_p(query, request, base_address, database, opts)
  end

  defp send_p(query, request, base_address, database, opts) do
    command = parse_command(query)

    query_data =
      case query.type do
        :select ->
          [request.query_string_data, " FORMAT ", @codec.response_format]

        _ ->
          [request.query_string_data]
      end

    post_data =
      case query.external_data do
        [] -> request.post_data
        data -> {:multipart, external_data_parts(data)}
      end

    params = %{
      database: database,
      query: IO.iodata_to_binary(query_data)
    }

    http_opts =
      case query.external_data do
        [] -> Keyword.put(opts, :params, params)
        data -> Keyword.put(opts, :params, Map.merge(params, external_data_params(data)))
      end

    with {:ok, %{status_code: 200, body: body}} <-
           HTTPoison.post(base_address, post_data, @req_headers, http_opts),
         {:command, :selected} <- {:command, command},
         {:ok, %{column_names: column_names, rows: rows}} <- @codec.decode(body) do
      {command, column_names, rows}
    else
      {:command, :updated} ->
        {:updated, 1}

      {:ok, response} ->
        {:error, response.body}

      {:error, error} ->
        {:error, error.reason}
    end
  end

  defp parse_command(%Query{type: :select}) do
    :selected
  end

  defp parse_command(_) do
    :updated
  end

  defp external_data_parts(data) do
    Enum.map(data, fn item ->
      {item.name, item.data, {"form-data", [name: item.name, filename: item.name]}, []}
    end)
  end

  defp external_data_params(data) do
    Enum.reduce(data, %{}, fn param, params ->
      Map.merge(params, %{
        "#{param.name}_structure" => param.structure,
        "#{param.name}_format" => param.format
      })
    end)
  end
end
