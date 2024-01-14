import sys
import boto3
import json
import logging
import traceback
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.types import DateType
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# Definindo padroes de log
MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger("JobDatameshPixSOT")
logger.setLevel(logging.INFO)

# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_SOR_PATH",
        "GLUE_INPUT_DATABASE",
        "GLUE_INPUT_TABLE",
        "S3_OUTPUT_SOT_PATH",
        "S3_OUTPUT_STAGE_PATH",
        "GLUE_OUTPUT_DATABASE",
        "GLUE_OUTPUT_TABLE",
        "DEBUG_MODE"
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

glue_client = boto3.client("glue", region_name="sa-east-1")
dynamo_cliente = boto3.client("dynamodb", region_name="sa-east-1")

input_sor = args["S3_INPUT_SOR_PATH"]
glue_input_database=args["GLUE_INPUT_DATABASE"],
glue_input_table=args["GLUE_INPUT_TABLE"],
output_sot = args["S3_OUTPUT_SOT_PATH"]
output_stage = args["S3_OUTPUT_STAGE_PATH"]
glue_output_database = args["GLUE_OUTPUT_DATABASE"]
glue_output_table = args["GLUE_OUTPUT_TABLE"]
debug_mode = args["DEBUG_MODE"]

sor_table = "tbdy5579_trsf_spi_teste_mock_input_sor"
one_big_table_sot_table = "tb_pix_one_big_table"
indiretos_table = "tbdy5578_pati_spi"

now = datetime.now()
d = now - timedelta(hours=4)
mes_atual = now.strftime("%Y-%m")
# partition = f"year={d.year}/month={d.month}/day={d.day}/hour={d.hour}"
partition = "year=2023/month=10/day=7/hour=12/minute=17"

s3_input_sor_path = f"s3://{input_sor}/{sor_table}/{partition}/"
indiretos_cache = f"s3://{output_stage}/participantes-indiretos-{mes_atual}-cache"

s3_output_one_big_table_path = f"s3://{output_sot}/{one_big_table_sot_table}"
job.init(args["JOB_NAME"], args)


# Build one big table columns
one_big_table_columns = dict(
    cod_idef_orig_trsf_finn_insn="cod_idef_orig_trsf_finn_insn",
    cod_situ_trsf_spi="cod_situ_trsf_spi",
    num_idef_cpvt="txt_mens_itgr_trsf_spi_dados_comprovante_emitido_chancela_comprovante",
    cod_moti_devo_trsf_pix="txt_mens_itgr_trsf_spi_dados_devolucao_motivo_motivo",
    txt_outr_moti_devo_trsf_pix="txt_mens_itgr_trsf_spi_dados_devolucao_motivo_motivo_descricao",
    cod_moti_trsf_pix_rjtd="txt_mens_trsf_spi_transacao_error_codigo",
    txt_moti_trsf_pix_rjtd="txt_mens_trsf_spi_transacao_error_mensagem",
    cod_moed="txt_mens_trsf_spi_moeda",
    cod_cana_dtbc="txt_mens_trsf_spi_codigo_canal",
    cod_idef_devo_trsf_pix="txt_mens_itgr_trsf_spi_dados_devolucao_id_devolucao",
    vlr_trsf_finn_insn="txt_mens_trsf_spi_valor",
    cod_idef_agem_pix="cod_idef_agem_spi",
    nom_situ_rete_trsf_pix="txt_mens_trsf_spi_status_retencao",
    cod_chav_dict="txt_mens_trsf_spi_creditos_meio_pagamento_dados_chave_dict",
    cod_idef_img_pgto_insn="txt_mens_trsf_spi_id_qrcode",
    cod_tipo_iniz_pix="txt_mens_trsf_spi_iniciacao_tipo",
    nom_tipo_chav_dict="txt_mens_trsf_spi_creditos_meio_pagamento_tipo_chave",
    dat_hor_cria_trsf_pix="dat_hor_cria_trsf_finn_insn",
    cod_idt_devo_pix_oril="txt_mens_trsf_spi_dados_med_id_fim_a_fim_med_devolucao_original",
    nom_situ_trsf_pix="nom_situ_trsf_pix",
    ind_cont_trnd="txt_mens_itgr_trsf_spi_ind_cont_trnd",
    cod_moti_lanc_cred_trsf="txt_mens_trsf_spi_creditos_literal",
    cod_moti_lanc_debt_trsf="txt_mens_trsf_spi_debitos_literal",
    nom_tipo_oper_finn="txt_mens_trsf_spi_nom_tipo_oper_finn",
    cod_idt_rcra_agem_pgto="txt_mens_itgr_trsf_spi_dados_recorrencia_codigo_recorrencia",
    vlr_saqu_trsf_finn_insn="txt_mens_trsf_spi_dados_saque_troco_valor_saque",
    vlr_trco_trsf_finn_insn="txt_mens_trsf_spi_dados_saque_troco_valor_compra",
    cod_tipo_aget_pix_saqu_trco="txt_mens_trsf_spi_dados_saque_troco_tipo_agente",
    cod_idef_estb="txt_mens_trsf_spi_dados_saque_troco_ispb_agente",
    num_cpf_cnpj_orig_iniz="txt_mens_trsf_spi_iniciacao_iniciador_documento",
    cod_idt_ccli_rcbr_iniz="txt_mens_trsf_spi_iniciacao_iniciador_id_conciliacao",
    nom_solt_iniz="txt_mens_trsf_spi_iniciacao_iniciador_nome",
    cod_crdl_solt_iniz="txt_mens_trsf_spi_iniciacao_iniciador_id",
    nom_tipo_devo="txt_mens_itgr_trsf_spi_dados_devolucao_motivo_tipo_med",
    cod_tipo_trsf_pix="txt_mens_trsf_spi_tipo_transferencia",
    txt_info_trsf_pix="txt_mens_trsf_spi_dados_lote_informacao_entre_usuarios",
    cod_idef_bloq_cutr_trsf_finn="txt_mens_itgr_trsf_spi_dados_bloqueio_cautelar_id",
    nom_tipo_trsf="txt_mens_trsf_spi_tipo_transferencia",
    num_agen_pati_idir="txt_mens_itgr_trsf_spi_dados_indireto_dados_debito_agencia",
    num_cont_pati_idir="txt_mens_itgr_trsf_spi_dados_indireto_dados_debito_conta",
    cod_pati_spi_emio="txt_mens_trsf_spi_debitos_meio_pagamento_dados_dados_conta_id_sistema_pagamento_brasileiro",
    num_agen_emio="txt_mens_trsf_spi_debitos_meio_pagamento_dados_dados_conta_agencia",
    num_cont_emio="txt_mens_trsf_spi_debitos_meio_pagamento_dados_dados_conta_conta",
    num_cmpl_cont_emio="txt_mens_trsf_spi_debitos_meio_pagamento_dados_dados_conta_complemento",
    num_docm_cpf_cnpj_emio="txt_mens_trsf_spi_debitos_identificacao_numero_documento",
    num_unic_cont_emio="txt_mens_trsf_spi_debitos_meio_pagamento_dados_dados_conta_codigo_conta",
    cod_tipo_cont_emio="txt_mens_trsf_spi_debitos_meio_pagamento_tipo",
    cod_tipo_pess_emio="txt_mens_trsf_spi_debitos_identificacao_tipo_pessoa",
    nom_pess_emio="txt_mens_trsf_spi_debitos_identificacao_nome",
    cod_idef_pess_emio="txt_mens_itgr_trsf_spi_dados_cadastro_conta_numero_unico_cliente",
    cod_pati_spi_favo="txt_mens_trsf_spi_creditos_meio_pagamento_dados_dados_conta_id_sistema_pagamento_brasileiro",
    num_agen_favo="txt_mens_trsf_spi_creditos_meio_pagamento_dados_dados_conta_agencia",
    num_cont_favo="txt_mens_trsf_spi_creditos_meio_pagamento_dados_dados_conta_conta",
    num_cmpl_cont_favo="txt_mens_trsf_spi_creditos_meio_pagamento_dados_dados_conta_complemento",
    num_docm_cpf_cnpj_favo="txt_mens_trsf_spi_creditos_identificacao_numero_documento",
    num_unic_cont_favo="txt_mens_trsf_spi_creditos_meio_pagamento_dados_dados_conta_codigo_conta",
    cod_tipo_cont_favo="txt_mens_trsf_spi_creditos_meio_pagamento_tipo",
    cod_tipo_pess_favo="txt_mens_trsf_spi_creditos_identificacao_tipo_pessoa",
    nom_pess_favo="txt_mens_trsf_spi_creditos_identificacao_nome",
    cod_idef_pess_favo="txt_mens_itgr_trsf_spi_dados_cadastro_conta_credito_numero_unico_cliente",
    dat_hor_cria_devo_trsf_pix="txt_mens_itgr_trsf_spi_dados_devolucao_data_evento",
    vlr_devo_trsf_finn_insn="txt_mens_itgr_trsf_spi_dados_devolucao_valor",
    cod_iniz_trsf_pix="txt_mens_trsf_spi_iniciacao_tipo"
)

fact_add_cols = [
    "cod_idef_trsf_spi",
    "dat_trsf_pix",
    "year",
    "month",
    "day",
    "hour",
]


def main():
    try:
        if debug_mode:
            print(f"s3_input_sor_path: {s3_input_sor_path}\n")
            print(f"s3_output_one_big_table_path: {s3_output_one_big_table_path}\n")
            print(f"glue_output_database: {glue_output_database}\n")
            print(f"glue_output_table: {glue_output_table}\n")

        #
        # ------------------------ 1 - Carrega o Dataframe da SoR -------------------------
        #
        df_sor = read_s3(s3_input_sor_path)
        if debug_mode:
            print(f"Count SOR: {df_sor.count()}")
            df_sor.show(4)
            
        # print(f"Schema SOR: \n")
        # df_sor.printSchema()
        #
        # ------------------------ 2 - Tratamento das colunas SoR -------------------------
        #
        if df_sor.count() > 0:            
            df_sor = convert_types(
                glue_client,
                spark,
                "db_corp_pagamentos_pixpagamentoinstantaneo_sor_01",
                "tbdy5579_trsf_spi",
                df_sor,
            )  # tabela de entrada
            df_sor = df_sor.withColumn("cod_idef_trsf_spi_liqa", lit("60701190"))
            # Flatten DataFrame
            df_sor = flatten(df_sor)
            df_sor = df_sor.withColumn(
                "txt_mens_trsf_spi_debitos", explode_outer("txt_mens_trsf_spi_debitos")
            )  # Transforma lista em mais linhas
            df_sor = df_sor.withColumn(
                "txt_mens_trsf_spi_creditos", explode_outer("txt_mens_trsf_spi_creditos")
            )  # Transforma lista em mais linhas
            df_sor = flatten(df_sor)
            # Carrega Informações dos Indiretos
            df_sor = add_indiretos(df_sor)

            # Cria partitions
            df_sor = df_sor.withColumn("year", lit(d.year))
            df_sor = df_sor.withColumn("month", lit(d.month))
            df_sor = df_sor.withColumn("day", lit(d.day))
            df_sor = df_sor.withColumn("hour", lit(d.hour))
            df_sor.persist()
            if debug_mode:
                print(f"Count SoR: {df_sor.count()}")

            #
            # -------------------------------- 3 - One big table para camada SOT ----------------------------------
            #
            df_one_big_table = map_columns(
                df_sor, one_big_table_columns
            )  # monta colunas da fato com df_sor gerando df_fact
            df_one_big_table = select_cols(
                df_one_big_table, one_big_table_columns, fact_add_cols
            )

            # cast da coluna vlr_trsf_finn_insn
            df_one_big_table = (
                df_one_big_table.withColumn(
                    "vlr_trsf_finn_insn", col("vlr_trsf_finn_insn").cast("string")
                )
            )

            df_one_big_table.persist()
            if debug_mode:
                print(f"Count One Big Table: {df_one_big_table.count()}")

            logger.info("===Realizar gravacao do conteudo na tabela da camada SOT===")
            bucket = "itau-corp-sot-sa-east-1-367064913884"
            prefix = "tb_pix_one_big_table"
            #clear_s3_path(bucket,prefix)
            write_one_big_table(df_one_big_table, "append",
                        # Teste para dynamic_frame no catalog
                        glue_context=glueContext,
                        caminho_destino=s3_output_one_big_table_path,
                        database=glue_output_database,
                        particoes=[
                            "year",
                            "month",
                            "day",
                            "hour"
                        ],
                        tabela_destino=glue_output_table,)

            job.commit()        
        else:
            logger.info("=== Dataframe vazio ===")
        logger.info("=== Encerramento do job ===")

    except Exception as ex:
        logger.error(
            f"""[main] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex


#
# ------------------------ Corrige o problema de tipo -------------------------
#
def convert_types(glue_client, spark, database_name, table_name, data_stg):
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        descriptor = response["Table"]["StorageDescriptor"]["Columns"]
        # creating a dataframe
        columns = ["Name", "Type"]
        dataframe = spark.createDataFrame(descriptor, columns)
        fields_reg = dataframe.collect()
        for row in fields_reg:
            data_stg = data_stg.withColumn(row["Name"], col(row["Name"]).cast(row["Type"]))
        return data_stg
    except Exception as ex:
        logger.error(
            f"""[convert_types] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex


#
# -------------------------- Flatten no DataFrame ----------------------------
#
def flatten(nested_df):
    try:
        stack = [((), nested_df)]
        columns = []
        while len(stack) > 0:
            parents, df = stack.pop()
            flat_cols = [
                col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
                for c in df.dtypes
                if c[1][:6] != "struct"
            ]
            nested_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]
            columns.extend(flat_cols)
            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))
        return nested_df.select(columns)
    except Exception as ex:
        logger.error(
            f"""[flatten] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex


#
# ----------------- Seleciona somente as colunas necessarias -----------------
#
def select_cols(df, table, _cols):
    try:
        columns = []
        for key in table:
            columns.append(key)
        for key in _cols:
            columns.append(key)

        needed = []
        for key in columns:
            if key in df.columns:
                needed.append(key)

        return df.select(*needed)
    except Exception as ex:
        logger.error(
            f"""[select_cols] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex


#
# ------------------------ Carrega os Indiretos -------------------------
#
def add_indiretos(df):
    tipoEvento = dict(
        SIMULACAO_PAGAMENTO_ERRO="RJCT",
        AUTORIZACAO_PAGAMENTO_ERRO="RJCT",
        EFETIVACAO_PAGAMENTO_ERRO="RJCT",
        EFETIVACAO_PAGAMENTO_ANALISE_FRAUDE_CANCELADA="RJCT",
        EFETIVACAO_PAGAMENTO_ANALISE_FRAUDE_REJEITADA="RJCT",
        CONFIRMACAO_PAGAMENTO_ERRO="RJCT",
        ENRIQUECIMENTO_PAGAMENTO_REJEITADO="RJCT",
        FINALIZACAO_DEBITO_PROCESSADA="ACSC",
        FINALIZACAO_DEBITO_REJEICAO_PROCESSADA="RJCT",
        FINALIZACAO_CREDITO_PROCESSADA="ACCC",
        SIMULACAO_DEVOLUCAO_ERRO="RJCT",
        AUTORIZACAO_DEVOLUCAO_ERRO="RJCT",
        EFETIVACAO_DEVOLUCAO_ERRO="RJCT",
        CONFIRMACAO_DEVOLUCAO_ERRO="RJCT",
        ENRIQUECIMENTO_DEVOLUCAO_REJEITADA="RJCT",
        FINALIZACAO_DEVOLUCAO_DEBITO_PROCESSADA="ACSC",
        FINALIZACAO_DEVOLUCAO_DEBITO_REJEICAO_PROCESSADA="RJCT",
        FINALIZACAO_DEVOLUCAO_CREDITO_PROCESSADA="ACCC",
        FINALIZACAO_DEBITO_REJEICAO_ERRO="RJCT",
        FINALIZACAO_DEVOLUCAO_DEBITO_REJEICAO_ERRO="RJCT",
        EFETIVACAO_PAGAMENTO_INDIRETO_ERRO="RJCT",
        ENRIQUECIMENTO_PAGAMENTO_INDIRETO_REJEITADO="RJCT",
        FINALIZACAO_DEBITO_PAGAMENTO_INDIRETO_PROCESSADA="ACSC",
        FINALIZACAO_CREDITO_PAGAMENTO_INDIRETO_PROCESSADA="ACCC",
        EFETIVACAO_DEVOLUCAO_INDIRETO_ERRO="RJCT",
        ENRIQUECIMENTO_DEVOLUCAO_INDIRETO_REJEITADA="RJCT",
        FINALIZACAO_DEBITO_DEVOLUCAO_INDIRETO_PROCESSADA="ACSC",
        FINALIZACAO_CREDITO_DEVOLUCAO_INDIRETO_PROCESSADA="ACCC",
        FINALIZACAO_DEVOLUCAO_FRAUDE_PROCESSADA="ACSC",
    )

    def getTipoOperFin(ispb_debito, ispb_credito):
        if ispb_debito is not None and ispb_credito is not None:
            if (ispb_debito == "60701190") | (ispb_debito in list_indiretos_cache) and (
                ispb_credito == "60701190"
            ) | (ispb_credito in list_indiretos_cache):
                return "intrabancario"
            return "interbancario"

    def getIndContTrnd(tipo_oper, credito, debito):
        result = ""
        if tipo_oper == "intrabancario":
            if credito is not None:
                result = credito
        else:
            if debito is not None:
                result = debito

        return result

    def getByStatus(status):
        for item in tipoEvento:  # finalização, crédito, erro
            if status.startswith(item):
                return tipoEvento[item]
        return ""

    def load_indiretos():
        list_indiretos_cache = []
        try:
            # Try to load -cache
            list_cache = glueContext.create_data_frame.from_options(
                connection_type="s3",
                format="json",
                connection_options={"paths": [indiretos_cache], "recurse": True},
            ).collect()
            for value in list_cache:
                list_indiretos_cache.append(value)
            list_indiretos_cache = list_indiretos_cache[0].ispb_participante
        except Exception as e:
            list_cache = None

        if not list_cache:
            response = dynamo_cliente.scan(
                ExpressionAttributeNames={
                    "#A": "cod_pati_spi",
                },
                ExpressionAttributeValues={
                    ":a": {
                        "S": "60701190",
                    },
                    ":b": {
                        "S": "true",
                    },
                },
                FilterExpression="cod_spb = :a and ind_pati_spi_ativ = :b",
                ProjectionExpression="#A",
                TableName=indiretos_table,
            )

            items = response.get("Items", [])
            if debug_mode:
                print("Lista indiretos recuperada do dynamodb")
                print(f"--- inicio items ---: \n{items}\n--- fim items ---")
            for item in items:
                valor_pk = item.get("cod_pati_spi", {}).get("S")
                list_indiretos_cache.append(valor_pk)

            if list_indiretos_cache:
                json_data = json.dumps(list_indiretos_cache)
                if debug_mode:
                    print(f"--- inicio json_data ---: \n{json_data}\n--- fim json_data ---")
                data_frame = spark.createDataFrame(
                    [(json_data,)], ["ispb_participante"]
                )
                data_frame.show()
                data_frame.coalesce(1).write.json(indiretos_cache)
                if debug_mode:
                    print("Lista indiretos gravada no s3")

        return list_indiretos_cache

    try:
        list_indiretos_cache = load_indiretos()
        getTipoOperFinUDF = udf(lambda z, x: getTipoOperFin(z, x), StringType())
        getIndContTrndUDF = udf(lambda z, x, y: getIndContTrnd(z, x, y), StringType())
        getCodSituUDF = udf(lambda z: getByStatus(z), StringType())

        df = df.withColumn(
            "txt_mens_trsf_spi_nom_tipo_oper_finn",
            getTipoOperFinUDF(
                col(
                    "txt_mens_trsf_spi_creditos_meio_pagamento_dados_dados_conta_id_sistema_pagamento_brasileiro"
                ),
                col(
                    "txt_mens_trsf_spi_debitos_meio_pagamento_dados_dados_conta_id_sistema_pagamento_brasileiro"
                ),
            ),
        )
        df = df.withColumn(
            "txt_mens_itgr_trsf_spi_ind_cont_trnd",
            getIndContTrndUDF(
                col("txt_mens_trsf_spi_nom_tipo_oper_finn"),
                col(
                    "txt_mens_itgr_trsf_spi_dados_cadastro_conta_credito_codigo_conta_transferida"
                ),
                col("txt_mens_itgr_trsf_spi_dados_cadastro_conta_codigo_conta_transferida"),
            ),
        )
        df = df.withColumn("nom_situ_trsf_pix", getCodSituUDF(col("cod_situ_trsf_spi")))

        return df
    except Exception as ex:
        logger.error(
            f"""[add_indiretos] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex


#
# -------------------------- Map Columns --------------------------
#
def map_columns(df, table):
    """
    This function will replace the columns names
    """
    try:
        for key in table:
            df = df.withColumnRenamed(table[key], key)
        return df
    except Exception as ex:
        logger.error(
            f"""[map_columns] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex


#
# -------------------------------- Read S3 ---------------------------------
#
def read_s3(s3_path):
    try:
        #"(year=='2017' and month=='04')"
        # partition = "year=2023/month=10/day=7/hour=12/minute=17"
        ano_current = datetime.now().strftime("%Y")
        mes_current = datetime.now().strftime("%m")
        dia_current = datetime.now().strftime("%d")
        hora_current = datetime.now().strftime("%H")
        minuto_current = datetime.now().strftime("%M")

        date_predicate = f"""year = {ano_current} 
            and month = {mes_current} 
            and day = {dia_current}
            and hour = {hora_current}
            and minute = {minuto_current}"""
        logger.info(f"***Data do processamento*** \n{date_predicate}")
        logger.info(f"***s3_path: *** \n{s3_path}")
        
        # trocar para o from catalog
        df = glueContext.create_dynamic_frame.from_options(
            format_options={},
            connection_type="s3",
            format="parquet",
            connection_options={
                "paths": [s3_path],
                "recurse": True,
            },
            transformation_ctx="df",
        )
        return df.toDF()
        
        # df = glueContext.create_data_frame.from_catalog(
        #                     database=glue_input_database,
        #                     table_name=glue_input_table,
        #                     push_down_predicate=date_predicate,
        #                     additional_options={
        #                         "useSparkDataSource": True,
        #                         "useCatalogSchema": True
        #                     }
        # )
        # return df
    except Exception as ex:
        logger.error(
            f"""[read_s3] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex

#
# ----------------------------- Grava One Big Table -----------------------------
#
def write_one_big_table(df, mode,
                        # novos para dynamic_frame e Glue Catalog
                        glue_context: GlueContext, **kwargs: dict):
    """
    This function will performe write fact table
    """
    try:
        if debug_mode:
            print(f"Número de registros SOT: {df.count()}")
        caminho_destino = kwargs.get("caminho_destino")
        database = kwargs.get("database")
        particoes = kwargs.get("particoes")
        tabela_destino = kwargs.get("tabela_destino")

        if debug_mode:
            logger.info("==parâmetros recebidos em escrever_para_catalogo:")
            logger.info(f"{caminho_destino}")
            logger.info(f"{database}")
            logger.info(f"{particoes}")
            logger.info(f"{tabela_destino}")

        # logger.info("== Gravação com getSink ==")
        # print(f"Schema SOT: \n")
        # df.printSchema()        
        # dynamicframe = DynamicFrame.fromDF(
        #      df, glue_context, "conteudo_para_gravacao"
        # )
        # tabela_glue = glue_context.getSink(
        #     connection_type="s3",
        #     path=caminho_destino,
        #     enableUpdateCatalog=True,
        #     updateBehavior="UPDATE_IN_DATABASE",
        #     partitionKeys=particoes,
        # )
        # tabela_glue.setFormat("glueparquet")
        # # testing overwrite mode 
        # tabela_glue.setCatalogInfo(
        #     catalogDatabase=database, catalogTableName=tabela_destino
        # )
        # logger.info(f"{dynamicframe.toDF().schema}")
        # tabela_glue.writeFrame(dynamicframe)

        logger.info("== Gravação com spark dataframe ==")
        if debug_mode:
            print(f"Schema SOT: \n")
            df.printSchema()
        df.write.format("parquet").mode(mode).partitionBy(
            "year", "month", "day", "hour"
        ).save(s3_output_one_big_table_path)
        
        MSCK REPAIR TABLE database.tabela_destino

        logger.info(
            f"""
            Gravação dos dados!
            Tabela : {database}.{tabela_destino}
            local da gravação:  {caminho_destino}
            """
        )
        return True

    except Exception as ex:
        logger.error(
            f"""[write_one_big_table] Erro ao executar job
        excecao: {ex}, detalhes: {traceback.print_exc()}"""
        )
        raise ex

if __name__ == "__main__":
    main()