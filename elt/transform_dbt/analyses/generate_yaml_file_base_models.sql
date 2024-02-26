{{ print('\nGenerate Base YAML File') }}
{{ print('---------------') }}
depends_on: {{ ref('base_noaa__catches') }}
depends_on: {{ ref('base_noaa__sizes') }}
depends_on: {{ ref('base_noaa__trips') }}

{% if execute %}
    {% set models_to_generate = codegen.get_models(directory=None, prefix='base_noaa') %}
    {{ codegen.generate_model_yaml(
        model_names = models_to_generate,
        upstream_descriptions=True,
        include_data_types=True
    ) }}
{% endif %}


