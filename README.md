# MVP — Engenharia de Dados

## Pipeline de Dados de Vendas com Databricks, Spark e Arquitetura Bronze, Silver e Gold

Este projeto foi desenvolvido como MVP da disciplina de Engenharia de Dados e tem como objetivo construir um pipeline completo utilizando **Databricks e Apache Spark**, contemplando coleta, armazenamento, análise de qualidade, transformação, modelagem dimensional, persistência e disponibilização dos dados para análise.

A solução foi estruturada segundo a arquitetura em camadas **Bronze, Silver e Gold**, permitindo separar as responsabilidades de ingestão, tratamento e consumo analítico dos dados.

---

# 1. Contexto de Negócios e Perguntas — Etapas 2 e 4.1

## 1.1 Contexto e objetivo

O conjunto de dados utilizado no projeto contém registros de vendas, incluindo informações relacionadas aos produtos comercializados, categorias, marcas, preços, custos, quantidades vendidas, clientes e localização geográfica das vendas.

O objetivo do MVP é desenvolver um pipeline de Engenharia de Dados capaz de transformar esses dados originalmente disponibilizados em arquivo CSV em informações tratadas, estruturadas, persistidas e adequadas para consumo analítico.

A arquitetura foi organizada da seguinte forma:

* **Bronze:** preservação dos dados próximos de seu estado original e realização das primeiras análises de qualidade;
* **Silver:** limpeza, padronização, correção dos tipos de dados, tratamento de duplicidades e enriquecimento;
* **Gold:** organização dos dados em modelo dimensional do tipo estrela (*Star Schema*) para consumo analítico.

As análises realizadas ao final do pipeline também funcionam como validação da arquitetura construída, demonstrando que os dados processados podem ser utilizados para responder diferentes questões de negócio.

## 1.2 Perguntas de negócio

Foram definidas cinco perguntas:

1. Quais categorias geram maior faturamento?
2. Quais continentes possuem maior volume de vendas?
3. Existe relação entre preço e quantidade vendida?
4. Quais produtos apresentam melhor desempenho?
5. Como as vendas evoluem ao longo do tempo?

## 1.3 Estrutura dos dados brutos

| Atributo        | Descrição                                      |
| --------------- | ---------------------------------------------- |
| `DataVenda`     | Data em que ocorreu a venda                    |
| `Produto`       | Nome do produto comercializado                 |
| `Categoria`     | Categoria do produto                           |
| `PrecoUnitario` | Preço unitário de venda                        |
| `CustoUnitario` | Custo unitário do produto                      |
| `Marca`         | Marca do produto                               |
| `Qtd_Vendida`   | Quantidade de unidades vendidas                |
| `NomeCliente`   | Nome do cliente                                |
| `Pais`          | País associado à venda                         |
| `Continente`    | Continente associado à venda                   |
| `Unnamed: 10`   | Coluna sem conteúdo útil identificada na fonte |

O arquivo possuía inicialmente **203.888 registros**, antes da aplicação dos tratamentos de qualidade.

## 1.4 Origem e licença dos dados

O conjunto de dados foi disponibilizado para utilização acadêmica durante o curso, com autorização do instrutor, sendo posteriormente armazenado no repositório GitHub utilizado pelo projeto.

Não foi identificada uma licença pública específica associada ao conjunto de dados. Dessa forma, sua utilização neste MVP está vinculada à finalidade acadêmica para a qual os dados foram disponibilizados.

---

# 2. Carga dos Dados — Etapa 4.2

## 2.1 Coleta

A coleta é realizada diretamente a partir do arquivo `Vendas.CSV` armazenado no repositório GitHub do projeto.

A leitura inicial é realizada utilizando **Pandas**, com `;` como separador e codificação `ISO-8859-1`. Em seguida, o DataFrame Pandas é convertido para um DataFrame Spark para processamento no Databricks.

### Código utilizado na coleta

```python
def importar_dataset():
    urlDados = 'https://raw.githubusercontent.com/marciomalrj/MVPED/main/Vendas.CSV'

    df = pd.read_csv(
        urlDados,
        sep=";",
        encoding="ISO-8859-1"
    )

    return df

df_spark = spark.createDataFrame(importar_dataset())

display(df_spark.limit(5))
```

O fluxo inicial é:

**Vendas.CSV → Pandas → Spark DataFrame → Bronze**

---

## 2.2 Persistência da Bronze

A camada Bronze preserva os dados próximos de sua estrutura original.

Durante a persistência foi necessário realizar uma adequação técnica no nome da coluna `Unnamed: 10`, renomeando-a temporariamente para `Unnamed_10`, pois o nome original contém caracteres não aceitos na persistência utilizada. O conteúdo da coluna não foi alterado nesta etapa.

### Código de persistência da Bronze

```python
df_bronze_persistencia = df_spark.withColumnRenamed(
    "Unnamed: 10",
    "Unnamed_10"
)

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.mvp_eng_dados
""")

df_bronze_persistencia.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.mvp_eng_dados.bronze_vendas")

df_bronze_persistida = spark.read.table(
    "workspace.mvp_eng_dados.bronze_vendas"
)

print(
    f"Registros na Bronze persistida: "
    f"{df_bronze_persistida.count()}"
)
```

<img width="253" height="215" alt="image" src="https://github.com/user-attachments/assets/625ba114-c365-43c5-828b-3e37d28c1f62" />

Persistência da tabela `bronze_vendas` no Databricks

O código completo está disponível no notebook:

`MVP - Engenharia de Dados.ipynb`

**Repositório:** `https://github.com/marciomalrj/MVPED/`

---

# 3. Modelagem e Catálogo de Dados — Etapa 4.3

## 3.1 Modelo dimensional

Para a camada Gold foi adotado um **Esquema Estrela (Star Schema)**.

O grão da tabela fato corresponde a uma ocorrência de venda de um produto em determinada data e localização.

O modelo é composto por:

* `gold_fato_vendas`;
* `gold_dim_produto`;
* `gold_dim_tempo`;
* `gold_dim_localizacao`.

A tabela fato concentra as métricas quantitativas e financeiras, enquanto as dimensões fornecem os atributos descritivos necessários para as análises.

```text
                         gold_dim_tempo
                                |
                                |
gold_dim_produto -------- gold_fato_vendas -------- gold_dim_localizacao
```

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/4fc1c33e-9ac8-442a-8e92-3ba47058e5cf" />

Modelo dimensional / MER da camada Gold

---

## 3.2 Construção da dimensão Produto

A dimensão Produto contém as combinações distintas de produto, categoria e marca. Foi criada a chave substituta `id_produto`.

### Código

```python
window_produto = Window.orderBy(
    "Produto",
    "Categoria",
    "Marca"
)

dim_produto = (
    df_gold_base
    .select(
        "Produto",
        "Categoria",
        "Marca"
    )
    .distinct()
    .withColumn(
        "id_produto",
        F.row_number().over(window_produto)
    )
    .select(
        "id_produto",
        "Produto",
        "Categoria",
        "Marca"
    )
)
```

---

## 3.3 Construção da dimensão Tempo

A dimensão Tempo é construída a partir das datas distintas das vendas. Além da chave substituta, foram derivados dia, mês e ano.

### Código

```python
window_tempo = Window.orderBy("DataVenda")

dim_tempo = (
    df_gold_base
    .select("DataVenda")
    .distinct()
    .withColumn(
        "id_tempo",
        F.row_number().over(window_tempo)
    )
    .withColumn("dia", F.dayofmonth("DataVenda"))
    .withColumn("mes", F.month("DataVenda"))
    .withColumn("ano", F.year("DataVenda"))
    .select(
        "id_tempo",
        "DataVenda",
        "dia",
        "mes",
        "ano"
    )
)
```

---

## 3.4 Construção da dimensão Localização

A dimensão Localização reúne as combinações distintas de país e continente.

### Código

```python
window_localizacao = Window.orderBy(
    "Pais",
    "Continente"
)

dim_localizacao = (
    df_gold_base
    .select(
        "Pais",
        "Continente"
    )
    .distinct()
    .withColumn(
        "id_localizacao",
        F.row_number().over(window_localizacao)
    )
    .select(
        "id_localizacao",
        "Pais",
        "Continente"
    )
)
```

---

## 3.5 Construção da tabela Fato Vendas

A tabela fato é construída relacionando a Silver com as três dimensões.

Os atributos descritivos utilizados nos relacionamentos são substituídos pelas respectivas chaves das dimensões.

### Código

```python
fato_vendas = (
    df_gold_base
    .join(
        dim_produto,
        on=["Produto", "Categoria", "Marca"],
        how="left"
    )
    .join(
        dim_tempo,
        on=["DataVenda"],
        how="left"
    )
    .join(
        dim_localizacao,
        on=["Pais", "Continente"],
        how="left"
    )
    .select(
        "id_produto",
        "id_tempo",
        "id_localizacao",
        F.col("Qtd_Vendida").alias("quantidade"),
        F.col("PrecoUnitario").alias("preco_unitario"),
        F.col("CustoUnitario").alias("custo_unitario"),
        F.col("Faturamento").alias("faturamento"),
        F.col("CustoTotal").alias("custo_total"),
        F.col("Lucro").alias("lucro")
    )
)
```

---

## 3.6 Catálogo — `silver_vendas`

A `silver_vendas` representa os dados tratados e preparados para alimentar a camada Gold.

| Atributo        | Tipo    | Descrição              | Domínio / Faixa observada                                          |
| --------------- | ------- | ---------------------- | ------------------------------------------------------------------ |
| `DataVenda`     | date    | Data da venda          | Período entre 2017 e 2019                                          |
| `Produto`       | string  | Produto comercializado | Produtos existentes na base                                        |
| `Categoria`     | string  | Categoria do produto   | Categorias existentes na base                                      |
| `PrecoUnitario` | double  | Preço unitário         | R$ 4,98 a R$ 1.650,00                                              |
| `CustoUnitario` | double  | Custo unitário         | R$ 2,54 a R$ 546,68                                                |
| `Marca`         | string  | Marca do produto       | Marcas existentes na base                                          |
| `Qtd_Vendida`   | integer | Quantidade vendida     | 1 a 5                                                              |
| `NomeCliente`   | string  | Nome do cliente        | Clientes existentes na base                                        |
| `Pais`          | string  | País da venda          | Países existentes na base                                          |
| `Continente`    | string  | Continente da venda    | África, América do Norte, América do Sul, Ásia, Austrália e Europa |
| `Faturamento`   | double  | Receita da venda       | R$ 4,98 a R$ 8.250,00                                              |
| `CustoTotal`    | double  | Custo total            | R$ 2,54 a R$ 2.733,40                                              |
| `Lucro`         | double  | Resultado financeiro   | R$ 2,44 a R$ 5.516,60                                              |

---

## 3.7 Catálogo — `gold_dim_produto`

| Atributo     | Tipo    | Descrição        | Linhagem                  |
| ------------ | ------- | ---------------- | ------------------------- |
| `id_produto` | integer | Chave substituta | Gerada na Gold            |
| `Produto`    | string  | Nome do produto  | `silver_vendas.Produto`   |
| `Categoria`  | string  | Categoria        | `silver_vendas.Categoria` |
| `Marca`      | string  | Marca            | `silver_vendas.Marca`     |

---

## 3.8 Catálogo — `gold_dim_tempo`

| Atributo    | Tipo    | Descrição        | Domínio                  | Linhagem                  |
| ----------- | ------- | ---------------- | ------------------------ | ------------------------- |
| `id_tempo`  | integer | Chave substituta | Inteiro positivo e único | Gerada na Gold            |
| `DataVenda` | date    | Data da venda    | 2017–2019                | `silver_vendas.DataVenda` |
| `dia`       | integer | Dia              | 1–31                     | Derivado de `DataVenda`   |
| `mes`       | integer | Mês              | 1–12                     | Derivado de `DataVenda`   |
| `ano`       | integer | Ano              | 2017–2019                | Derivado de `DataVenda`   |

---

## 3.9 Catálogo — `gold_dim_localizacao`

| Atributo         | Tipo    | Descrição        | Domínio                                                            | Linhagem                   |
| ---------------- | ------- | ---------------- | ------------------------------------------------------------------ | -------------------------- |
| `id_localizacao` | integer | Chave substituta | Inteiro positivo e único                                           | Gerada na Gold             |
| `Pais`           | string  | País da venda    | Países existentes                                                  | `silver_vendas.Pais`       |
| `Continente`     | string  | Continente       | África, América do Norte, América do Sul, Ásia, Austrália e Europa | `silver_vendas.Continente` |

---

## 3.10 Catálogo — `gold_fato_vendas`

| Atributo         | Tipo    | Descrição            | Domínio / Faixa observada     | Linhagem             |
| ---------------- | ------- | -------------------- | ----------------------------- | -------------------- |
| `id_produto`     | integer | FK Produto           | IDs de `gold_dim_produto`     | Dimensão Produto     |
| `id_tempo`       | integer | FK Tempo             | IDs de `gold_dim_tempo`       | Dimensão Tempo       |
| `id_localizacao` | integer | FK Localização       | IDs de `gold_dim_localizacao` | Dimensão Localização |
| `quantidade`     | integer | Quantidade vendida   | 1 a 5                         | `Qtd_Vendida`        |
| `preco_unitario` | double  | Preço unitário       | R$ 4,98 a R$ 1.650,00         | `PrecoUnitario`      |
| `custo_unitario` | double  | Custo unitário       | R$ 2,54 a R$ 546,68           | `CustoUnitario`      |
| `faturamento`    | double  | Receita              | R$ 4,98 a R$ 8.250,00         | `Faturamento`        |
| `custo_total`    | double  | Custo total          | R$ 2,54 a R$ 2.733,40         | `CustoTotal`         |
| `lucro`          | double  | Resultado financeiro | R$ 2,44 a R$ 5.516,60         | `Lucro`              |

---

## 3.11 Linhagem

A linhagem principal é:

**Vendas.CSV → Pandas → Spark → Bronze → Silver → Gold → Análises**

O atributo `NomeCliente` permanece disponível na Silver, mas não foi incorporado à Gold, pois as perguntas definidas neste MVP não exigem análises no nível do cliente.

<img width="250" height="227" alt="image" src="https://github.com/user-attachments/assets/b8a64072-c2de-462a-b47b-f77314d4cf0f" />

Catálogo/schema das tabelas no Databricks

---

# 4. Pipeline de Dados — Etapa 4.4

O pipeline foi desenvolvido em um único notebook Databricks e dividido logicamente nas camadas Bronze, Silver e Gold.

## 4.1 Bronze

A Bronze recebe os dados da fonte, preservando-os próximos de sua estrutura original. Nesta camada foram realizadas verificações de qualidade para identificar problemas antes das transformações.

A persistência ocorre em formato Delta na tabela:

`workspace.mvp_eng_dados.bronze_vendas`

---

## 4.2 Silver

A Silver executa os tratamentos identificados na Bronze.

### Remoção da coluna vazia

```python
df_silver = df_spark.drop("Unnamed: 10")
```

### Remoção das linhas completamente vazias

```python
condicao_linha_vazia = F.expr(
    " AND ".join([
        f"({c} IS NULL OR TRIM(CAST({c} AS STRING)) = '')"
        for c in df_silver.columns
    ])
)

df_silver = df_silver.filter(
    ~condicao_linha_vazia
)
```

### Remoção das duplicidades técnicas

```python
df_silver = df_silver.dropDuplicates()
```

### Padronização da Marca

```python
df_silver = df_silver.withColumn(
    "Marca",
    F.trim(F.col("Marca"))
)
```

### Conversão dos tipos

```python
df_silver = (
    df_silver

    .withColumn(
        "DataVenda",
        F.to_date(F.col("DataVenda"), "dd/MM/yyyy")
    )

    .withColumn(
        "PrecoUnitario",
        F.regexp_replace(
            F.col("PrecoUnitario"), ",", "."
        ).cast("double")
    )

    .withColumn(
        "CustoUnitario",
        F.regexp_replace(
            F.col("CustoUnitario"), ",", "."
        ).cast("double")
    )

    .withColumn(
        "Qtd_Vendida",
        F.col("Qtd_Vendida").cast("int")
    )
)
```

### Criação do Faturamento

```python
df_silver = df_silver.withColumn(
    "Faturamento",
    F.round(
        F.col("PrecoUnitario") *
        F.col("Qtd_Vendida"),
        2
    )
)
```

### Criação do Custo Total

```python
df_silver = df_silver.withColumn(
    "CustoTotal",
    F.round(
        F.col("CustoUnitario") *
        F.col("Qtd_Vendida"),
        2
    )
)
```

### Criação do Lucro

```python
df_silver = df_silver.withColumn(
    "Lucro",
    F.round(
        F.col("Faturamento") -
        F.col("CustoTotal"),
        2
    )
)
```

### Persistência da Silver

```python
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.mvp_eng_dados.silver_vendas"
    )
```

---

## 4.3 Gold

A Gold começa pela leitura da Silver persistida:

```python
df_gold_base = spark.read.table(
    "workspace.mvp_eng_dados.silver_vendas"
)
```

Após a construção das dimensões e da tabela fato apresentadas na seção de Modelagem, as quatro estruturas são persistidas.

### Código de persistência

```python
dim_produto.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.mvp_eng_dados.gold_dim_produto"
    )

dim_tempo.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.mvp_eng_dados.gold_dim_tempo"
    )

dim_localizacao.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.mvp_eng_dados.gold_dim_localizacao"
    )

fato_vendas.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.mvp_eng_dados.gold_fato_vendas"
    )
```

A existência das tabelas é posteriormente validada no próprio Databricks:

```python
display(
    spark.sql("""
        SHOW TABLES IN workspace.mvp_eng_dados
    """)
)
```

<img width="226" height="228" alt="image" src="https://github.com/user-attachments/assets/05a8f6d5-6255-4521-b64e-363e90ef30d2" />


Tabelas persistidas no Databricks**

> O ideal é mostrar `bronze_vendas`, `silver_vendas`, `gold_dim_produto`, `gold_dim_tempo`, `gold_dim_localizacao` e `gold_fato_vendas`.

O fluxo completo é:

```text
Vendas.CSV
    ↓
Pandas
    ↓
Spark
    ↓
BRONZE
bronze_vendas
    ↓
SILVER
silver_vendas
    ↓
GOLD
├── gold_dim_produto
├── gold_dim_tempo
├── gold_dim_localizacao
└── gold_fato_vendas
    ↓
Análises de Negócio
```

---

# 5. Qualidade de Dados — Etapa 4.5

A qualidade dos dados foi analisada inicialmente na Bronze.

| Problema                    | Evidência                      | Tratamento                                       |
| --------------------------- | ------------------------------ | ------------------------------------------------ |
| Coluna sem conteúdo útil    | `Unnamed: 10`                  | Removida                                         |
| Linhas completamente vazias | 6 registros                    | Removidas                                        |
| `DataVenda` inadequada      | `string`                       | Convertida para `date`                           |
| `PrecoUnitario` inadequado  | `string`                       | Convertido para `double`                         |
| `CustoUnitario` inadequado  | `string`                       | Convertido para `double`                         |
| `Qtd_Vendida` inadequada    | `double`                       | Convertida para `integer`                        |
| Espaços excedentes          | 14.200 ocorrências em `Marca`  | Aplicação de `trim()`                            |
| Duplicidades                | 32.794 registros identificados | Investigação e remoção das duplicidades técnicas |

As duplicidades receberam atenção especial porque a base não possui identificador único de transação. Dessa forma, os registros não foram removidos automaticamente apenas por apresentarem valores semelhantes.

A investigação identificou registros completamente idênticos em todos os atributos, considerados forte indício de duplicidade técnica. Esses registros foram tratados na Silver utilizando `dropDuplicates()`.

Os principais códigos utilizados nos tratamentos estão documentados na seção **Pipeline de Dados**, e as células completas de investigação e validação permanecem disponíveis no notebook.

---

# 6. Análise de Dados — Etapa 4.5

## 6.1 Quais categorias geram maior faturamento?

A análise relaciona a tabela fato com a dimensão Produto e agrega o faturamento por categoria.

### Código

```python
gold_fato_vendas = spark.read.table(
    "workspace.mvp_eng_dados.gold_fato_vendas"
)

gold_dim_produto = spark.read.table(
    "workspace.mvp_eng_dados.gold_dim_produto"
)

faturamento_categoria = (
    gold_fato_vendas
    .join(
        gold_dim_produto,
        on="id_produto",
        how="inner"
    )
    .groupBy("Categoria")
    .agg(
        F.round(
            F.sum("faturamento"),
            2
        ).alias("Faturamento")
    )
    .orderBy(F.desc("Faturamento"))
)

display(faturamento_categoria)
```

A categoria **Câmeras Digitais SLR** apresentou o maior faturamento, com aproximadamente **R$ 11,59 milhões**, seguida por **Acessórios para Câmeras**, com aproximadamente **R$ 11,08 milhões**.

Na sequência aparecem **VCD & DVD**, com aproximadamente **R$ 5,69 milhões**, e **Games**, com aproximadamente **R$ 5,19 milhões**.

Os resultados demonstram concentração relevante do faturamento em determinadas categorias.

<img width="990" height="400" alt="image" src="https://github.com/user-attachments/assets/915c9dd0-273e-4991-804a-d94abb422e69" />


Faturamento por Categoria

---

## 6.2 Quais continentes possuem maior volume de vendas?

### Código

```python
gold_dim_localizacao = spark.read.table(
    "workspace.mvp_eng_dados.gold_dim_localizacao"
)

volume_continente = (
    gold_fato_vendas
    .join(
        gold_dim_localizacao,
        on="id_localizacao",
        how="inner"
    )
    .groupBy("Continente")
    .agg(
        F.sum("quantidade")
        .alias("Quantidade_Vendida")
    )
    .orderBy(
        F.desc("Quantidade_Vendida")
    )
)

quantidade_total = (
    volume_continente
    .agg(F.sum("Quantidade_Vendida"))
    .first()[0]
)

volume_continente = (
    volume_continente
    .withColumn(
        "Participacao_Percentual",
        F.round(
            (
                F.col("Quantidade_Vendida") /
                quantidade_total
            ) * 100,
            2
        )
    )
)

display(volume_continente)
```

A **América do Norte** concentra pouco mais de **50%** do total de unidades vendidas. A **Europa** aparece na segunda posição, com aproximadamente **26%**, seguida pela **América do Sul**, com cerca de **13%**.

Os resultados demonstram uma distribuição geográfica desigual, com predominância da América do Norte.

<img width="1055" height="400" alt="image" src="https://github.com/user-attachments/assets/bba1222b-ba87-4051-bb73-e9473a26bee6" />


Participação das Vendas por Continente

---

## 6.3 Existe relação entre preço e quantidade vendida?

Foi utilizado o coeficiente de correlação de Pearson.

### Código

```python
correlacao = gold_fato_vendas.stat.corr(
    "preco_unitario",
    "quantidade"
)

print(
    f"Correlação entre preço unitário e "
    f"quantidade vendida: {correlacao:.4f}"
)
```

Para a visualização:

```python
preco_quantidade = (
    gold_fato_vendas
    .select(
        "preco_unitario",
        "quantidade"
    )
)

display(preco_quantidade)
```

O resultado obtido foi:

**Correlação = -0,0014**

Por estar extremamente próximo de zero, o resultado indica ausência de relação linear significativa entre preço unitário e quantidade vendida.

O gráfico de dispersão reforça a interpretação, pois as quantidades aparecem distribuídas pelas diferentes faixas de preço sem tendência clara.

<img width="1182" height="400" alt="image" src="https://github.com/user-attachments/assets/172cfc1c-5534-4021-8adf-25b1043216c5" />


Preço Unitário × Quantidade Vendida

---

## 6.4 Quais produtos apresentam melhor desempenho?

Foi adotado como critério de desempenho o **faturamento total gerado pelo produto**.

### Código

```python
desempenho_produtos = (
    gold_fato_vendas
    .join(
        gold_dim_produto,
        on="id_produto",
        how="inner"
    )
    .groupBy("Produto")
    .agg(
        F.round(
            F.sum("faturamento"),
            2
        ).alias("Faturamento")
    )
    .orderBy(
        F.desc("Faturamento")
    )
)

display(
    desempenho_produtos.limit(10)
)
```

A consulta produz o ranking dos produtos por faturamento, sendo apresentados os **10 produtos com melhor desempenho**.

Além de responder à pergunta, a consulta demonstra a aplicação prática do relacionamento entre a tabela fato e a dimensão Produto.

<img width="1182" height="400" alt="image" src="https://github.com/user-attachments/assets/b5094485-7051-47f7-b7cc-cd8f49438683" />


Top 10 Produtos por Faturamento

---

## 6.5 Como as vendas evoluem ao longo do tempo?

A análise utiliza a dimensão Tempo para agregar quantidade e faturamento por ano e mês.

### Código

```python
gold_dim_tempo = spark.read.table(
    "workspace.mvp_eng_dados.gold_dim_tempo"
)

evolucao_vendas_mensal = (
    gold_fato_vendas
    .join(
        gold_dim_tempo,
        on="id_tempo",
        how="inner"
    )
    .groupBy(
        "ano",
        "mes"
    )
    .agg(
        F.sum("quantidade")
        .alias("Quantidade_Vendida"),

        F.round(
            F.sum("faturamento"),
            2
        ).alias("Faturamento")
    )
    .withColumn(
        "Ano_Mes",
        F.concat(
            F.col("ano"),
            F.lit("-"),
            F.lpad(F.col("mes"), 2, "0")
        )
    )
    .orderBy(
        "ano",
        "mes"
    )
)

display(evolucao_vendas_mensal)
```

O faturamento apresentou crescimento entre o final de **2017** e o início de **2018**, atingindo seu maior nível no início de 2018, próximo de **R$ 3 milhões mensais**.

Após esse período observa-se uma tendência geral de redução durante 2018, embora existam oscilações mensais.

Em **2019**, os valores permanecem inferiores aos observados durante grande parte de 2018.

<img width="1182" height="400" alt="image" src="https://github.com/user-attachments/assets/5f2fbe7e-2f63-4030-9a98-55e6b747b5cc" />


Evolução Mensal do Faturamento — 2017 a 2019

---

## 6.6 Conclusão Geral das Análises

As cinco perguntas demonstraram que o modelo dimensional construído na Gold permite analisar os dados sob diferentes perspectivas.

Foi possível identificar as categorias responsáveis pelos maiores faturamentos, analisar a distribuição geográfica das vendas, verificar a relação entre preço e quantidade, identificar os produtos com maior desempenho e acompanhar a evolução das vendas ao longo do tempo.

Mais do que os resultados analíticos individualmente, essas análises validam o pipeline desenvolvido. Os dados originalmente disponibilizados em CSV passaram por processos de ingestão, qualidade, tratamento, enriquecimento, persistência e modelagem antes de serem utilizados para responder às perguntas de negócio.

O processo pode ser resumido como:

**Dados brutos → Qualidade → Transformação → Modelagem → Persistência → Informação analítica**

---

# 7. Autoavaliação

Ao final do desenvolvimento deste MVP, considero que os objetivos inicialmente propostos foram atingidos. Foi possível construir um pipeline completo de Engenharia de Dados no ambiente Databricks, contemplando coleta, armazenamento, análise de qualidade, transformação, persistência, modelagem e disponibilização dos dados para consultas analíticas.

A utilização da arquitetura Bronze, Silver e Gold contribuiu para compreender, na prática, a responsabilidade de cada etapa de um pipeline. A Bronze permitiu preservar os dados próximos de sua origem e identificar problemas de qualidade; a Silver concentrou tratamentos, padronizações e enriquecimentos; e a Gold possibilitou aplicar conceitos de modelagem dimensional através da construção de um esquema estrela.

Durante o desenvolvimento, algumas dificuldades exigiram maior atenção, principalmente o tratamento dos tipos originalmente incompatíveis com o significado dos atributos, a investigação dos registros duplicados, a padronização dos campos textuais e os ajustes necessários para persistência das tabelas Delta no Databricks.

A construção da Gold também representou um aprendizado importante, principalmente na definição do grão da tabela fato, criação das chaves substitutas e estabelecimento dos relacionamentos entre fatos e dimensões.

Outro desafio foi manter a rastreabilidade das transformações realizadas. A elaboração do Catálogo de Dados e da linhagem contribuiu para consolidar esse entendimento, permitindo acompanhar como os atributos presentes no modelo analítico foram produzidos a partir dos dados das camadas anteriores.

Como trabalhos futuros, o pipeline poderá ser dividido em notebooks ou tarefas independentes e posteriormente orquestrado utilizando **Databricks Workflows**, permitindo automatizar a execução das camadas e controlar suas dependências.

Também poderão ser implementados mecanismos adicionais de monitoramento e validação automática da qualidade dos dados, além da integração de novas fontes de informação.

O modelo dimensional poderá ainda ser expandido com novas dimensões e métricas, assim como poderão ser desenvolvidos dashboards para disponibilização dos indicadores produzidos pela Gold.

De forma geral, este MVP proporcionou a aplicação prática dos principais conceitos estudados em Engenharia de Dados, consolidando conhecimentos relacionados a **Spark, Databricks, arquitetura em camadas, qualidade de dados, Delta Lake, linhagem e modelagem dimensional**.

---

# 8. Tecnologias Utilizadas

* Databricks
* Apache Spark
* PySpark
* Python
* Pandas
* Delta Lake
* GitHub
* Star Schema

---

# 9. Evidências da Execução

As evidências da execução do pipeline e das análises são apresentadas ao longo deste documento por meio de screenshots obtidos diretamente no Databricks.

Foram previstas evidências para:

* persistência das tabelas Bronze, Silver e Gold;
* modelo dimensional da Gold;
* estrutura/schema das tabelas;
* verificações de qualidade;
* faturamento por categoria;
* volume de vendas por continente;
* relação entre preço e quantidade;
* Top 10 produtos por faturamento;
* evolução mensal do faturamento.

---

# 10. Conclusão

O MVP demonstrou a construção de um pipeline completo de Engenharia de Dados, desde a ingestão dos dados brutos em formato CSV até sua disponibilização em uma camada analítica estruturada.

A arquitetura **Bronze → Silver → Gold** permitiu organizar as responsabilidades de ingestão, tratamento e consumo dos dados. Spark e Databricks foram utilizados nas transformações e na persistência das diferentes etapas em formato Delta, enquanto o modelo dimensional da Gold tornou os dados adequados para responder às perguntas de negócio.

Os códigos apresentados neste README evidenciam as principais etapas de implementação, enquanto o notebook disponibilizado no repositório contém o processo completo, incluindo as verificações intermediárias e validações executadas durante o desenvolvimento.

As análises realizadas ao final confirmaram que a estrutura desenvolvida é capaz de transformar dados brutos em informações organizadas e adequadas para consumo analítico, atingindo os objetivos definidos para o MVP.
