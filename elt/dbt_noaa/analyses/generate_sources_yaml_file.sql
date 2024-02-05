{{ print('\nGenerate Source YAML File') }}
{{ print('---------------') }}
{{ codegen.generate_source(schema_name= 'raw', 
                            database_name= 'noaa_dw',
                            generate_columns= true,
                            include_descriptions= true,
                            include_database= true,
                            include_schema= true) }}
