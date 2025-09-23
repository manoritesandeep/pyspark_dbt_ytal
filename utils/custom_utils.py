from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, row_number, col, current_timestamp
from delta.tables import DeltaTable

class transformations:

    def dedup(self, df:DataFrame, dedup_cols:List, cdc:str):

        dedup_list = []

        df = df.withColumn("dedupKey",concat(*dedup_cols))
        df = df.withColumn(
            "dedupCounts",
            row_number().over(
                Window.partitionBy("dedupKey").orderBy(col(cdc).desc())
            )
        )
        df = df.filter(col('dedupCounts')==1)
        df = df.drop("dedupKey","dedupCounts")

        return df

    def process_timestamp(self, df):

        df = df.withColumn("process_timestamp", current_timestamp())

        return df
    
    # def upsert(self, df, key_cols, table, cdc):
    #     merge_condition = " AND ".join([f"src.{i} = tgt.{i}" for i in key_cols])
    #     dlt_obj = DeltaTable.forName(spark, f"pyspark.silver.{table}")
    #     dlt_obj.alias("trg").merge(df.alias("src"), merge_condition)\
    #                 .whenMatchedUpdateAll(condition="src.{cdc} > trg.{cdc}")\
    #                 .whenNotMatchedInsertAll()\
    #                 .execute()
        
    #     return "Upsert done!"

    def upsert(self, df, key_cols, table, cdc):
        merge_condition = " AND ".join([f"src.{col} = trg.{col}" for col in key_cols])
        dlt_obj = DeltaTable.forName(f"pyspark_dbt_project.silver.{table}")
        dlt_obj.alias("trg").merge(
            source=df.alias("src"),
            condition=merge_condition
        ).whenMatchedUpdateAll(
            condition=f"src.{cdc} > trg.{cdc}"
        ).whenNotMatchedInsertAll().execute()
        return "Upsert done!"