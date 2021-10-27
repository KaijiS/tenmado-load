from jinja2 import Template


def embed_to_query(query_base: str, params: dict[str, any]) -> str:

    # jinja2テンプレートでレンダリング準備
    template = Template(query_base)

    # レンダリング
    query = template.render(params)

    return query
