import os
import asyncio
from neo4j import GraphDatabase
from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.experimental.pipeline.kg_builder import SimpleKGPipeline
from neo4j_graphrag.llm import OpenAILLM
from dotenv import load_dotenv

load_dotenv()
def get_env_variable(name):
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"에러: {name} 변수를 .env에서 찾을 수 없습니다. 파일 위치나 변수명을 확인하세요.")
    return value.strip()
neo4j_uri = get_env_variable("NEO4J_URI")
neo4j_user = get_env_variable("NEO4J_USERNAME")
neo4j_password = get_env_variable("NEO4J_PASSWORD")

driver = GraphDatabase.driver(
    neo4j_uri, 
    auth=(neo4j_user, neo4j_password)
)
node_types = ["Person", "House", "Planet"]
relationship_types = ["PARENT_OF", "HEIR_OF", "RULES"]
patterns = [
    ("Person", "PARENT_OF", "Person"),
    ("Person", "HEIR_OF", "House"),
    ("House", "RULES", "Planet"),
]

embedder = OpenAIEmbeddings(model="text-embedding-3-large")

llm = OpenAILLM(
    model_name="gpt-4o",
    model_params={
        "max_tokens": 2000,
        "response_format": {"type": "json_object"},
        "temperature": 0,
    },
)

kg_builder = SimpleKGPipeline(
    llm=llm,
    driver=driver,
    embedder=embedder,
    schema={
        "node_types": node_types,
        "relationship_types": relationship_types,
        "patterns": patterns,
    },
    on_error="IGNORE",
    from_pdf=False,
)

text = (
    "The son of Duke Leto Atreides and the Lady Jessica, Paul is the heir of House "
    "Atreides, an aristocratic family that rules the planet Caladan."
)
asyncio.run(kg_builder.run_async(text=text))
driver.close()