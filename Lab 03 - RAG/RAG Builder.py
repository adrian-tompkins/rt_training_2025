# Databricks notebook source
# MAGIC %pip install -U --quiet databricks-sdk==0.36.0 databricks-agents mlflow-skinny==2.18.0 mlflow==2.18.0 mlflow[gateway]==2.18.0 databricks-vectorsearch langchain==0.2.1 langchain_core==0.2.5 langchain_community==0.2.4 transformers==4.41.1 pypdf==4.1.0 langchain-text-splitters==0.2.0 tiktoken==0.7.0 llama-index==0.10.43
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/helper

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration
# MAGIC
# MAGIC Edit these values

# COMMAND ----------

# Name the RAG
rag_name = ""

# Specify the PDF Source
# https://www.riotinto.com/en/sustainability/climate-change/decarbonisation-progress
source_pdf = ""

# Choose the LLM model to use
llm_model = ""

# Choose our embedding model to use
embedding_model_endpoint_name = ""

# Specify the Databricks Vector Search Endpoint
vector_search_endpoint = ""

# Unity Catalog - Choose The Catalog
catalog = ""

# Schema for the rag
schema = ""

# Configure the maximum of results to match against in the vector search index
max_search_results = 5

# Configure the test question for this lab
test_question = ""


# COMMAND ----------

# MAGIC %md
# MAGIC #### Infered and fixed config. Do not change.
# MAGIC
# MAGIC TODO: move the llm_prompt_template to the configuration section (requires dynamic building of the "chain" notebook)

# COMMAND ----------

import re

llm_prompt_template = """You are an assistant that answers questions. Use the following pieces of retrieved context to answer the question. Some pieces of context may be irrelevant, in which case you should not use them to form the answer.\n\nContext: {context}"""
rag_name_sanitized = str.lower(re.sub(r'[^a-zA-Z0-9_]', '_', rag_name)).strip('_')
table_prefix = f"rag_{rag_name_sanitized}"
pdf_chunk_table_name = f"{table_prefix}_pdf_chunked"
vector_search_index_name = f"{table_prefix}_pdf_vector_index"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate the Configuration

# COMMAND ----------

import os

assert os.path.exists(source_pdf), f"The file {source_pdf} does not exist."
assert spark.sql(f"SHOW CATALOGS").filter(f"catalog = '{catalog}'").count() > 0, f"The catalog {catalog} does not exist."
spark.sql(f"USE CATALOG {catalog}")
assert spark.sql(f"SHOW SCHEMAS").filter(f"databaseName = '{schema}'").count() > 0, f"The schema {schema} does not exist."
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create / Retrieve The Vector Search Client

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient(disable_notice=True)

if not endpoint_exists(vsc, vector_search_endpoint):
    vsc.create_endpoint(name=vector_search_endpoint, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, vector_search_endpoint)
print(f"Endpoint named {vector_search_endpoint} is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest and chunk the PDF 

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {pdf_chunk_table_name} (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY,
  url STRING,
  content STRING
) TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")
spark.sql(f"TRUNCATE TABLE {pdf_chunk_table_name}")

# COMMAND ----------

from llama_index.core.node_parser import SentenceSplitter
from llama_index.core import Document, set_global_tokenizer
from transformers import AutoTokenizer
from typing import Iterator
import warnings
from pypdf import PdfReader
import io

def parse_bytes_pypdf(raw_doc_contents_bytes: bytes):
    #Note: in production setting you might want to try/catch errors here to handle incorrect pdf/files
    pdf = io.BytesIO(raw_doc_contents_bytes)
    reader = PdfReader(pdf)
    parsed_content = [page_content.extract_text() for page_content in reader.pages]
    return "\n".join(parsed_content)
# Reduce the arrow batch size as our PDF can be big in memory (classic compute only)
# spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10)

@pandas_udf("array<string>")
def read_as_chunk(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    #set llama2 as tokenizer to match our model size (will stay below gte 1024 limit)
    set_global_tokenizer(
      #AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer", cache_dir="/tmp/hf_cache")
      AutoTokenizer.from_pretrained("/Volumes/adrian_tompkins/rag_chatbot_private_model/respect_survey/llama-tokenizer", cache_dir="/tmp/hf_cache")
    )
    #Sentence splitter from llama_index to split on sentences
    splitter = SentenceSplitter(chunk_size=500, chunk_overlap=10)
    def extract_and_split(b):
      try:
        txt = parse_bytes_pypdf(b)
      except Exception as e:
        txt = f'__PDF_PARSING_ERROR__ file: {e}'
        print(txt)
      if txt is None:
        return []
      nodes = splitter.get_nodes_from_documents([Document(text=txt)])
      return [n.text for n in nodes]

    for x in batch_iter:
        yield x.apply(extract_and_split)

# COMMAND ----------

(spark.read.format("binaryFile").load(source_pdf)
      .withColumn("content", F.explode(read_as_chunk("content")))
      .filter("content not like '__PDF_PARSING_ERROR__%'") #Drop PDF with parsing ERROR (could throw an error instead or properly flag that in a prod setup to avoid silent failures)
      .selectExpr('path as url', 'content')
).write.mode("append").saveAsTable(pdf_chunk_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Vector Store

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

#The table we'd like to index
source_table_fullname = f"{catalog}.{schema}.{pdf_chunk_table_name}"
# Where we want to store our index
vs_index_fullname = f"{catalog}.{schema}.{vector_search_index_name}"

if not index_exists(vsc, vector_search_endpoint, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {vector_search_endpoint}...")
  vsc.create_delta_sync_index(
    endpoint_name=vector_search_endpoint,
    index_name=vs_index_fullname,
    source_table_name=source_table_fullname,
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column='content', #The column containing our text
    embedding_model_endpoint_name=embedding_model_endpoint_name #The embedding endpoint used to create the embeddings
  )
  #Let's wait for the index to be ready and all our embeddings to be created and indexed
  wait_for_index_to_be_ready(vsc, vector_search_endpoint, vs_index_fullname)
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  wait_for_index_to_be_ready(vsc, vector_search_endpoint, vs_index_fullname)
  vsc.get_index(vector_search_endpoint, vs_index_fullname).sync()

print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the RAG

# COMMAND ----------

chain_config = {
    "llm_model_serving_endpoint_name": llm_model,  # the foundation model we want to use
    "vector_search_endpoint_name": vector_search_endpoint,  # the endpoint we want to use for vector search
    "vector_search_index": f"{catalog}.{schema}.{vector_search_index_name}",
    "llm_prompt_template": llm_prompt_template,
    "max_search_results": max_search_results,
}

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain.schema.runnable import RunnableLambda
from langchain_core.output_parsers import StrOutputParser

## Enable MLflow Tracing
mlflow.langchain.autolog()

## Load the chain's configuration
model_config = mlflow.models.ModelConfig(development_config=chain_config)

## Turn the Vector Search index into a LangChain retriever
vs_client = VectorSearchClient(disable_notice=True)
vs_index = vs_client.get_index(
    endpoint_name=model_config.get("vector_search_endpoint_name"),
    index_name=model_config.get("vector_search_index"),
)
vector_search_as_retriever = DatabricksVectorSearch(
    vs_index,
    text_column="content",
        columns=["id", "content", "url"],
).as_retriever(search_kwargs={"k": max_search_results})

# Method to format the docs returned by the retriever into the prompt (keep only the text from chunks)
def format_context(docs):
    chunk_contents = [f"Passage: {d.page_content}\n" for d in docs]
    return "".join(chunk_contents)

#Let's try our retriever chain:
relevant_docs = (vector_search_as_retriever | RunnableLambda(format_context)| StrOutputParser()).invoke(test_question)

display_txt_as_html(relevant_docs)

# COMMAND ----------

from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks
from operator import itemgetter

prompt = ChatPromptTemplate.from_messages(
    [  
        ("system", model_config.get("llm_prompt_template")), # Contains the instructions from the configuration
        ("user", "{question}") #user's questions
    ]
)

# Our foundation model answering the final prompt
model = ChatDatabricks(
    endpoint=model_config.get("llm_model_serving_endpoint_name"),
    extra_params={"temperature": 0.01, "max_tokens": 500}
)

#Let's try our prompt:
answer = (prompt | model | StrOutputParser()).invoke({'question':test_question, 'context': ''})
display_txt_as_html(answer)

# COMMAND ----------

# Return the string contents of the most recent messages: [{...}] from the user to be used as input question
def extract_user_query_string(chat_messages_array):
    return chat_messages_array[-1]["content"]

# RAG Chain
chain = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
        "context": itemgetter("messages")
        | RunnableLambda(extract_user_query_string)
        | vector_search_as_retriever
        | RunnableLambda(format_context),
    }
    | prompt
    | model
    | StrOutputParser()
)

# COMMAND ----------

# Let's give it a try:
input_example = {"messages": [ {"role": "user", "content": test_question}]}
answer = chain.invoke(input_example)
print(answer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy a RAG Chain

# COMMAND ----------

# DBTITLE 1,Deploy the chain in Unity Catalog
# Log the model to MLflow
with mlflow.start_run(run_name=rag_name_sanitized):
  logged_chain_info = mlflow.langchain.log_model(
          #Note: In classical ML, MLflow works by serializing the model object.  In generative AI, chains often include Python packages that do not serialize.  Here, we use MLflow's new code-based logging, where we saved our chain under the chain notebook and will use this code instead of trying to serialize the object.
          lc_model=os.path.join(os.getcwd(), 'chain'),  # Chain code file e.g., /path/to/the/chain.py 
          model_config=chain_config, # Chain configuration 
          artifact_path="chain", # Required by MLflow, the chain's code/config are saved in this directory
          input_example=input_example,
          example_no_conversion=True,  # Required by MLflow to use the input_example as the chain's schema
      )

MODEL_NAME = rag_name_sanitized
MODEL_NAME_FQN = f"{catalog}.{schema}.{MODEL_NAME}"
# Register to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_chain_info.model_uri, name=MODEL_NAME_FQN)

# COMMAND ----------

from databricks import agents
# Deploy to enable the Review APP and create an API endpoint
# Note: scaling down to zero will provide unexpected behavior for the chat app. Set it to false for a prod-ready application.
deployment_info = agents.deploy(MODEL_NAME_FQN, model_version=uc_registered_model_info.version, scale_to_zero=True)

instructions_to_reviewer = f"""## Instructions for Testing the {rag_name} chatbot

Your inputs are invaluable for the development team. By providing detailed feedback and corrections, you help us fix issues and improve the overall quality of the application. We rely on your expertise to identify any gaps or areas needing enhancement."""

# Add the user-facing instructions to the Review App
agents.set_review_instructions(MODEL_NAME_FQN, instructions_to_reviewer)

wait_for_model_serving_endpoint_to_be_ready(deployment_info.endpoint_name)

# COMMAND ----------

print(f"\n\nReview App URL to share with your stakeholders: {deployment_info.review_app_url}")
