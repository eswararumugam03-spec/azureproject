# Databricks notebook source
parameters = [

    {
        "table" : "spotify.silver.factstream",
        "alias" : "factstream",
        "cols" : "factstream.stream_id,factstream.listen_duration"
    },
    {
        "table" : "spotify.silver.dimuser",
        "alias" : "dimuser",
        "cols" : "dimuser.user_id,dimuser.user_name",
        "condition" : "factstream.user_id = dimuser.user_id"
    },
    {
        "table" : "spotify.silver.dimtrack",
        "alias" : "dimtrack",
        "cols" : "dimtrack.track_id,dimtrack.track_name,dimtrack.album_name",
        "condition" : "factstream.track_id = dimtrack.track_id"
    }

]

# COMMAND ----------

pip install jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

query_text = """

            select 
                {% for param in parameters %}
                    {{param.cols}}
                        {% if not loop.last %}
                             ,
                        {% endif %}
                {% endfor %}
            from
                {% for param in parameters %}
                    {% if loop.first %}
                        {{param['table']}}
                    {% endif %}
                {% endfor %}
                {% for param in parameters %}
                    {% if not loop.first %}
                        left join 
                            {{param['table']}} as {{param['alias']}}
                        on 
                            {{param['condition']}}
                    {% endif %}
                {% endfor %}
                


"""

# COMMAND ----------

jinja_sql_srt = Template(query_text)
query = jinja_sql_srt.render(parameters=parameters)
print(query)

# COMMAND ----------

display(spark.sql(query))

# COMMAND ----------

