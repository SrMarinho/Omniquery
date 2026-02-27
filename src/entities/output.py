import time
import csv
import io
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
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
        Transfere dados do DuckDB para PostgreSQL de forma simples.
        """
        import io
        import time
        
        # Tempo de configuração
        config_start = time.time()
        conn = output_database.raw_connection()
        config_time = time.time() - config_start

        print(f"📤 Starting transfer: DuckDB ➔ PostgreSQL [Table: {name}]")
        print(f"⚙️  Settings: synchronous_commit=OFF | if_exists={self.options.get('if_exists', 'replace')}")
        print("─" * 60)
        
        try:
            cur = conn.cursor()
            cur.execute("SET synchronous_commit TO OFF")
            
            # Limpa tabela existente se necessário
            if self.options.get("if_exists", "replace") == "replace":
                cur.execute(f'DROP TABLE IF EXISTS {name}')
            
            # Executa query e obtém dados
            print("📊 Executing query...")
            query_start = time.time()
            result = source_database.execute(query)
            rows = result.fetchall()
            columns = [desc[0].lower() for desc in result.description]
            query_time = time.time() - query_start
            print(f"  Query executed: {len(rows):,} rows retrieved ⏱️  {query_time:.2f}s")
            
            # Cria tabela
            columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
            cur.execute(f'CREATE TABLE {name} ({columns_def})')
            
            # Prepara dados para COPY
            transfer_start = time.time()
            print(f"  Preparing data for transfer...")
            
            output = io.StringIO()
            data_size = 0
            for i, row in enumerate(rows, 1):
                line = '\t'.join(['\\N' if v is None else str(v) for v in row])
                data_size += len(line.encode('utf-8')) + 1  # +1 para o newline
                output.write(line + '\n')
                
                # Feedback a cada 500k linhas
                if i % 500000 == 0:
                    print(f"    Processed {i:,} rows...")
            
            output.seek(0)
            
            # COPY para PostgreSQL
            copy_start = time.time()
            cur.copy_from(output, name, sep='\t', null='\\N', columns=columns)
            conn.commit()
            copy_time = time.time() - copy_start
            
            # Otimiza
            cur.execute(f"ANALYZE {name}")
            conn.commit()
            
            transfer_time = time.time() - transfer_start
            total_time = time.time() - config_start
            
            total_rows = len(rows)
            avg_speed = total_rows / transfer_time
            data_size_mb = data_size / (1024 * 1024)
            speed_mb_s = data_size_mb / transfer_time if transfer_time > 0 else 0
            
            print("─" * 60)
            print(f"✅ Transfer completed successfully!")
            print(f"📊 Final summary for table {name}:")
            print(f"   • Total records:     {total_rows:>15,}")
            print(f"   • Data size:         {data_size_mb:>15.2f} MB")
            print(f"   • Transfer time:     {transfer_time:>15.2f}s")
            print(f"   • Average speed:     {avg_speed:>15,.0f} rows/s  ({speed_mb_s:.2f} MB/s)")
            print("\n")
            
        except Exception as e:
            print("❌" * 30)
            print(f"🔴 ERROR transferring data for {name}")
            print(f"⚠️  Details: {str(e)}")
            print(f"📋 Records processed before error: {len(rows) if 'rows' in locals() else 0:,}")
            print("❌" * 30)
            
        finally:
            cur.close()
            conn.close()

    def run(self) -> None:
        """Executa a transferência de dados do DuckDB para o banco de dados de destino."""
        job_start = time.time()
        
        print("─" * 60)
        print(f"🚀 Starting job: {self.name}")
        print(f"📋 Target: {self.output_database} › Table: {self.name}")
        print(f"🔍 Query: {self.query[:100]}{'...' if len(self.query) > 100 else ''}")
        print("─" * 60)
        
        try:
            output_connection_string: str = get_database_config(self.output_database)["connection_string"]
            output_engine = create_engine(output_connection_string)
            
            self._transfer(memory_database, output_engine, self.name, self.query)
            
            job_time = time.time() - job_start
            print("─" * 60)
            print(f"✅ Job completed successfully: {self.name}")
            print(f"📊 Destination: {self.output_database} › {self.name}")
            print(f"⏱️  Total time: {job_time:.2f}s")
            print("─" * 60)
            
        except Exception as e:
            job_time = time.time() - job_start
            print("❌" * 30)
            print(f"🔴 JOB FAILED: {self.name}")
            print(f"⚠️  Target: {self.output_database} › {self.name}")
            print(f"⚠️  Error: {str(e)}")
            print(f"⏱️  Failed after: {job_time:.2f}s")
            print("❌" * 30)
            raise

class FileOutput(Output):
    type: str = "file" 

    def _transfer(self, source_engine: DuckDBPyConnection, file_path: str, query: str) -> None:
        """
        Transfere dados do DuckDB para arquivo CSV em chunks.
        """
        from pathlib import Path
        import time
        import pandas as pd
        
        filepath = Path(file_path)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Tempo de configuração
        config_start = time.time()
        chunk_size = 100000
        
        print(f"📤 Starting transfer: DuckDB ➔ CSV [File: {filepath.name}]")
        print(f"⚙️  Settings: chunk_size={chunk_size:,} | location={file_path}")
        print("─" * 60)
        
        total_rows = 0
        transfer_start = time.time()
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
                    operation = "🆗 Created"
                    first_chunk = False
                else:
                    chunk_df.to_csv(filepath, mode="a", index=False, header=False)
                    operation = "➕ Appended"
                
                # Estatísticas do chunk
                chunk_time = time.time() - chunk_start
                total_rows += chunk_rows
                
                # Calcula velocidade (com segurança)
                if chunk_time > 0:
                    speed = chunk_rows / chunk_time
                    print(f"  Batch #{chunk_count:2d} {operation:12s} : {chunk_rows:10,} rows "
                        f"⏱️  {chunk_time:5.2f}s")
                else:
                    print(f"  Batch #{chunk_count:2d} {operation:12s} : {chunk_rows:10,} rows "
                        f"⏱️  {chunk_time:5.2f}s (transferência instantânea)")
                
                del chunk_df
            
            transfer_time = time.time() - transfer_start
            total_time = time.time() - config_start
            
            # Estatísticas finais
            if total_rows > 0:
                file_size = filepath.stat().st_size
                avg_speed = total_rows / transfer_time if transfer_time > 0 else 0
                file_size_mb = file_size / (1024 * 1024)
                speed_mb_s = file_size_mb / transfer_time if transfer_time > 0 else 0
                
                print("─" * 60)
                print(f"✅ Transfer completed successfully!")
                print(f"📊 Final summary for file {filepath.name}:")
                print(f"   • Total records:     {total_rows:>15,}")
                print(f"   • File size:         {file_size_mb:>15.2f} MB")
                print(f"   • Number of chunks:  {chunk_count:>15,}")
                print(f"   • Transfer time:     {transfer_time:>15.2f}s")
                print(f"   • Average speed:     {avg_speed:>15,.0f} rows/s ({speed_mb_s:.2f} MB/s)")
                print("\n")
            else:
                print("─" * 60)
                print(f"⚠️  WARNING: No data transferred for {filepath.name}")
                print("\n")
                
        except Exception as e:
            print("❌" * 30)
            print(f"🔴 ERROR transferring data to {filepath.name}")
            print(f"⚠️  Details: {str(e)}")
            print(f"📋 Records processed before error: {total_rows:,}")
            # Remove arquivo incompleto se houver erro
            if filepath.exists() and total_rows == 0:
                filepath.unlink()
                print(f"🗑️  Removed incomplete file: {filepath.name}")
            print("❌" * 30)
            raise


    def run(self) -> None:
        """Executa a transferência de dados."""
        
        print("─" * 60)
        print(f"🚀 Starting job: {self.name}")
        print("─" * 60)
        
        try:
            self._transfer(memory_database, self.name, self.query)
            print(f"✨ Job completed successfully: {self.name}")
            print("─" * 60)
            
        except Exception as e:
            print("❌" * 30)
            print(f"🔴 JOB FAILED: {self.name}")
            print(f"⚠️  Error: {str(e)}")
            print("❌" * 30)
            raise

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
