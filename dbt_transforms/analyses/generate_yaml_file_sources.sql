{{ print('\nGenerate Source YAML File') }}
{{ print('---------------') }}

{{ codegen.generate_source(schema_name= 'raw', 
                            database_name= 'noaa_dw',
                            table_names = ["catch","size","trip"],
                            generate_columns= true,
                            include_descriptions= true,
                            include_database= true,
                            include_schema= true) }}
