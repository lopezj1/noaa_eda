{{ print('\nGenerate Staging Model SQL Files') }}
{{ print('---------------') }}
{% set datasets = ["catch","size","trip"] %}

{% for dataset in datasets %}

    {{ codegen.generate_base_model(
    source_name='raw',
    table_name=dataset
    ) }}

{% endfor %}