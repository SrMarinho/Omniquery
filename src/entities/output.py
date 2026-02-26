import time
from pathlib import Path
from pydantic import BaseModel, Field, model_validator
from typing import Dict, Any, Type
from duckdb import DuckDBPyConnection
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import create_engine, Engine
from src.types.file_output_format_types import FileOutputFormatTypes
from src.utils.database_config_reader import get_database_config
from src.config import memory_database

class Output(BaseModel):
    type: str
    name: str
    query: str
    options: Dict = Field(default_factory=dict)

    def run(self) -> None:
        print(f"Writing output type: {type(self)}")

class DatabaseOutput(Output):
    type: str = "database" 
    output_database: str = Field(default="postgresql")
    
    def _transfer(self, source_database: DuckDBPyConnection, output_database: Engine, name: str, query: str) -> None:
        """
        Transfere dados em chunks do DuckDB para qualquer banco SQLAlchemy.
        """
        chunk_size = 500000
        total_rows = 0
        start_time = time.time()
        
        first_chunk = True
        chunk_count = 0
        
        # Executa a query no DuckDB
        cursor = source_database.execute(query)
        
        # Pega os nomes das colunas (convertendo para minúsculo)
        columns = [desc[0].lower() for desc in cursor.description]
        
        try:
            while True:
                # Busca um chunk de dados
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                    
                chunk_count += 1
                chunk_start = time.time()
                
                # Cria DataFrame com os dados do chunk
                chunk_df = pd.DataFrame(rows, columns=columns)
                
                print(f"🔄 Transferindo chunk {chunk_count}: {len(chunk_df)} linhas para tabela {name}")
                
                # Decide se é replace ou append
                if first_chunk:
                    chunk_df.to_sql(
                        name=name,
                        con=output_database,
                        if_exists=self.options.get("if_exists", "replace"),
                        index=False,
                        chunksize=chunk_size,
                        method='multi'
                    )
                    first_chunk = False
                else:
                    chunk_df.to_sql(
                        name=name,
                        con=output_database,
                        if_exists='append',
                        index=False,
                        chunksize=chunk_size,
                        method='multi'
                    )
                
                # Estatísticas do chunk
                chunk_time = time.time() - chunk_start
                total_rows += len(chunk_df)
                
                if chunk_time > 0:
                    speed = len(chunk_df) / chunk_time
                    print(f"  → {len(chunk_df):,} linhas em {chunk_time:.2f}s ({speed:,.0f} linhas/s)")
                
                del chunk_df
                
        except Exception as e:
            print(f"❌ Erro durante transferência: {e}")
            raise
        
        # Estatísticas finais
        total_time = time.time() - start_time
        print(f"\n✅ Transferência concluída para tabela {name}:")
        print(f"   📊 Total: {total_rows:,} linhas em {chunk_count} chunks")
        print(f"   ⏱️  Tempo total: {total_time:.2f}s")
        
        if total_time > 0 and total_rows > 0:
            avg_speed = total_rows / total_time
            print(f"   🚀 Velocidade média: {avg_speed:,.0f} linhas/s")

    def run(self) -> None:
        print(f"Writing in database: {self.output_database}, table: {self.name}")
        output_connection_string: str = get_database_config(self.output_database)["connection_string"]
        output_engine = create_engine(output_connection_string)
        self._transfer(memory_database, output_engine, self.name, self.query)

class FileOutput(Output):
    type: str = "file" 

    def _transfer(self, source_engine: DuckDBPyConnection, file_path: str, query: str) -> None:
        """
        Transfere dados do DuckDB para arquivo CSV em chunks.
        """
        filepath = Path(file_path)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        chunk_size = 100000
        total_rows = 0
        start_time = time.time()
        
        first_chunk = True
        chunk_count = 0
        
        # Executa a query e obtém o cursor
        cursor = source_engine.execute(query)
        
        # Pega os nomes das colunas
        columns = [desc[0] for desc in cursor.description]
        
        try:
            while True:
                # Busca um chunk de dados
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                    
                chunk_count += 1
                chunk_start = time.time()
                
                # Cria DataFrame com os dados do chunk
                chunk_df = pd.DataFrame(rows, columns=columns)
                
                # Normaliza nomes das colunas
                chunk_df.columns = chunk_df.columns.str.lower()
                chunk_rows = len(chunk_df)
                
                # Escreve chunk no CSV
                if first_chunk:
                    chunk_df.to_csv(filepath, index=False)
                    print(f"📁 Criando novo arquivo: {file_path}")
                    first_chunk = False
                else:
                    chunk_df.to_csv(filepath, mode="a", index=False, header=False)
                
                # Estatísticas do chunk
                chunk_time = time.time() - chunk_start
                total_rows += chunk_rows
                
                # Calcula velocidade (com segurança)
                if chunk_time > 0:
                    speed = chunk_rows / chunk_time
                    print(f"  → Chunk {chunk_count}: {chunk_rows:,} linhas em {chunk_time:.2f}s ({speed:,.0f} linhas/s)")
                else:
                    print(f"  → Chunk {chunk_count}: {chunk_rows:,} linhas (transferência instantânea)")
                
                del chunk_df
                
        except Exception as e:
            print(f"❌ Erro durante transferência: {e}")
            # Remove arquivo incompleto se houver erro
            if filepath.exists() and total_rows == 0:
                filepath.unlink()
            raise
        
        # Estatísticas finais
        if total_rows > 0:
            total_time = time.time() - start_time
            file_size = filepath.stat().st_size
            
            print(f"\n✅ Transferência concluída!")
            print(f"   📊 Total: {total_rows:,} linhas em {chunk_count} chunks")
            print(f"   💾 Tamanho: {file_size/1024/1024:.2f} MB")
            print(f"   ⏱️  Tempo total: {total_time:.2f}s")
            
            if total_time > 0:
                avg_speed = total_rows / total_time
                print(f"   🚀 Velocidade média: {avg_speed:,.0f} linhas/s")
        else:
            print(f"⚠️  Nenhum dado transferido para {file_path}")


    def run(self) -> None:
        print(f"Writing in file: {self.name}")
        self._transfer(memory_database, self.name, self.query)

class OutputFactory:
    output_types = {
        "database": DatabaseOutput,
        "file": FileOutput,
    }
    @staticmethod
    def create(config: Dict[str, Any]) -> Output:
        output_type: str = config.get("type", "")
        if output_type.lower() in OutputFactory.output_types:
            return OutputFactory.output_types[output_type.lower()](**config)
        
        return Output(**config)
