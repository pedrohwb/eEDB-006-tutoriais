# Tutorial Completo: AWS Athena com AWS Glue Data Catalog

> Guia passo a passo para criar tabelas no AWS Glue, carregar dados CSV (3 arquivos relacionais), executar queries SQL no Athena e gerar a tabela final de resultados.

---

## Sumario

1. [Visao Geral](#1-visao-geral)
2. [Arquitetura](#2-arquitetura)
3. [O que e AWS Athena?](#3-o-que-e-aws-athena)
4. [O que e AWS Glue?](#4-o-que-e-aws-glue)
5. [Pre-requisitos](#5-pre-requisitos)
6. [Estrutura do Projeto](#6-estrutura-do-projeto)
7. [Dados de Exemplo](#7-dados-de-exemplo)
8. [Passo 1 — Configurar credenciais AWS](#8-passo-1--configurar-credenciais-aws)
9. [Passo 2 — Terraform: Criar infraestrutura S3](#9-passo-2--terraform-criar-infraestrutura-s3)
10. [Passo 3 — Verificar S3](#10-passo-3--verificar-s3)
11. [Passo 4 — Criar tabelas no Glue Data Catalog](#11-passo-4--criar-tabelas-no-glue-data-catalog)
12. [Passo 5 — Executar queries no Athena](#12-passo-5--executar-queries-no-athena)
13. [Passo 6 — Ver os Resultados](#13-passo-6--ver-os-resultados)
14. [Passo 7 — Descomissionamento](#14-passo-7--descomissionamento)
15. [Custos e Orcamento](#15-custos-e-orcamento)
16. [Alternativa: Glue Crawler](#16-alternativa-glue-crawler)
17. [Troubleshooting](#17-troubleshooting)

---

## 1. Visao Geral

Neste tutorial voce vai criar um ambiente de **analytics serverless** usando AWS Athena e AWS Glue com os mesmos dados do tutorial anterior (clientes, produtos e vendas de uma loja virtual).

### O que voce vai aprender

- Criar infraestrutura AWS com **Terraform** (S3 bucket + upload dos CSVs)
- Entender a diferenca entre **AWS Glue** (catalogo de metadados) e **AWS Athena** (motor de queries SQL)
- Criar **tabelas no Glue** definindo schema fixo (structura das colunas)
- Escrever **queries SQL** no Athena para fazer JOIN entre 3 tabelas
- Entender como o Athena **escaneia dados no S3** sem copiar dados para um cluster
- Gerar uma **tabela de resultado** persistida no S3
- Descomissionar tudo para nao gerar custos

### Comparacao com o tutorial anterior (EMR Hive)

| Aspecto | EMR Hive (tutorial anterior) | Athena + Glue (este tutorial) |
|---|---|---|
| **Infraestrutura** | Cluster EC2 (2 instancias m4.large) | Zero EC2 - 100% serverless |
| **Custo/hora** | ~$0.20/hora (cluster) | ~$0.00 (pay-per-query) |
| **Setup** | 10-15 minutos ate cluster pronto | 2-3 minutos |
| **Gerenciamento** | Cluster management, SSH, bootstrap | Zero cluster para gerenciar |
| **Schema** | Definido via HQL no cluster | Definido via Glue API |
| **Execution engine** | MapReduce (Hadoop) | Apache Spark / Presto (por baixo do Athena) |
| **Dados** | Lidos do S3 ou HDFS interno | Lidos diretamente do S3 |

### Ferramentas usadas neste tutorial

| Ferramenta | Para que serve |
|---|---|
| **Terraform** | Criar infraestrutura como codigo (S3 bucket + uploads) |
| **AWS CLI** | Interagir com AWS services (S3, Glue, Athena) |
| **AWS S3** | Armazenamento de objetos (dados + resultados) |
| **AWS Glue** | Catalogo de dados (definir schema das tabelas) |
| **AWS Athena** | Motor de queries SQL serverless |

---

## 2. Arquitetura

```
Seu computador                     AWS Cloud (us-east-1)
=================                  =====================

Terraform                          +-----------------------------------+
                                  |   S3 Bucket                       |
                                  |   (dados + resultados)            |
                                  |                                   |
                                  |   data/clientes/clientes.csv       |
                                  |   data/produtos/produtos.csv       |
                                  |   data/vendas/vendas.csv          |
                                  |   results/resultado_vendas/       |
                                  |   athena-query-results/          |
                                  +--------+------------------------+
                                           |
                    +----------------------+----------------------+
                    |                                              |
              +-----+-----+                               +-------+-----+
              |  Glue      |                               |   Athena     |
              |  Data      |                               |   (query     |
              |  Catalog   |                               |    engine)   |
              |            |                               |             |
              | Schema:    |                               | SQL query   |
              |  clientes  |                               | JOIN +      |
              |  produtos  |                               | GROUP BY    |
              |  vendas    |                               | ORDER BY    |
              |  resultado |                               |             |
              +------------+                               +-------------+
                    |                                              |
                    |  Meta-informacao                             |  Lê dados
                    |  (schema)                                   |  do S3
                    +----------------------------------------------+
                                  |
                                  v
                         S3 (dados reais)
```

### Como funciona o fluxo

1. **Terraform** cria o bucket S3 e faz upload dos 3 CSVs
2. **Glue** armazena o schema (metadata) de cada tabela - apenas a estrutura, nao os dados
3. **Athena** usa o schema do Glue para saber como ler os arquivos no S3
4. Quando voce executa uma query, o Athena:
   - Le o schema no Glue
   - Vai no S3 e escaneia os arquivos CSV correspondentes
   - Executa o SQL (JOIN, GROUP BY, etc.)
   - Escreve o resultado em outra pasta no S3

### Por que分开 (separado) Schema e Dados?

No mundo tradicional de bancos de dados (MySQL, PostgreSQL), o schema e os dados estao juntos no mesmo lugar.

Com AWS Glue + Athena + S3, eles estao separados por design:

| Componente | O que faz | analogy |
|---|---|---|
| **S3** | Armazena os arquivos CSV (dados fisicos) | HD do computador |
| **Glue** | Armazena o schema (metadata) - nomes de colunas, tipos, localizacao no S3 | Indice de um livro |
| **Athena** | Le o schema no Glue, escaneia dados no S3, executa SQL | Pessoa que usa o indice para encontrar informacao |

**Vantagem**: O mesmo schema no Glue pode ser usado por outros servicos AWS (Redshift Spectrum, EMR, SageMaker, etc.). Os dados no S3 podem ser lidos por qualquer ferramenta.

---

## 3. O que e AWS Athena?

### Definicao simples

AWS Athena e um **servico de consultas SQL serverless** que permite analisar dados armazenados no S3 usando SQL padrao (Sintaxe mirip com PostgreSQL/MySQL).

### Caracteristicas principais

| Caracteristica | Explicacao |
|---|---|
| **Serverless** | Nao ha servidores para provisionar, gerenciar ou pagar por hora. Você paga apenas pelos dados escaneados. |
| **SQL padrao** | Queries em SQL (nao precisa saber programar MapReduce ou Spark) |
| **Lê do S3** | Os dados permanecem no S3. Athena escaneia e processa in-place. |
| **Sem carga ETL** | Não é necessário cargar os dados para um banco de dados separado. Consulta direto no S3. |
| **Resultados no S3** | Resultados das queries vao para uma pasta no S3 que voce especificar |

### Como o Athena funciona (por baixo dos panos)

O Athena usa **Apache Presto** (ou Amazon Presto) como motor de queries. Quando voce executa uma query:

1. O Athena recebe a query SQL
2. Parser converte SQL em um plano de execucao
3. O planner decide como escanear os dados no S3 (usa o schema do Glue)
4. Varias "worker threads" escaneiam partes diferentes do arquivo em paralelo
5. Os resultados sao agregados e devolvidos
6. O resultado final e escrito no S3 na pasta de output

### Preco do Athena

| O que e cobrado | Quanto |
|---|---|
| **Dados escaneados** | $5.00 por TB escaneado |
| **Queries bem-sucedidas** | Bilheteiros de sucesso so contam dados escaneados |
| **Queries que falham** | Nao sao cobradas |
| **Dados compactados** | Colunas com compression rendem menos dados escaneados = mais barato |

Para os nossos dados pequenos (~10KB), o custo sera ~$0.00.

### Quando usar Athena

- **Boa escolha**: Analise ad-hoc, BI leve, explorar dados em S3
- **M má escolha**: Queries em tempo real extreme (use Redshift), ETL pesado (use Glue ou EMR), grandes volumes sem compressao (custo pode crescer rapido)

---

## 4. O que e AWS Glue?

### Definicao simples

AWS Glue e um **catalogo de dados gerenciado** (data catalog) que armazena metadata sobre seus datasets no S3. Ele permite que voce defina o schema das suas tabelas sem Serverless.

### Glue vs Hive Metastore

Se voce ja conhece o tutorial de EMR Hive, o Glue e essentially o **Hive Metastore como um servico gerenciado**:

| Hive (no EMR) | AWS Glue |
|---|---|
| Hive Metastore (MySQL/PostgreSQL interno) | Glue Data Catalog (gerenciado pela AWS) |
| Roda no cluster EMR | Servico separado, acessivel por qualquer servico AWS |
| Tables armazenadas no Metastore | Tables armazenadas no Glue |
| Schema definido via HQL | Schema definido via Console, CLI ou API |

### Componentes do Glue

| Componente | O que faz |
|---|---|
| **Glue Data Catalog** | Repositorio central de metadata (tabelas, databases, views) |
| **Glue Crawler** | Automatiza a descoberta de schema (scaneia pastas e infere colunas) |
| **Glue ETL** | Jobs de transformacao de dados (nao usado neste tutorial) |
| **Glue DataBrew** | Visual data preparation (nao usado neste tutorial) |

### Preco do Glue

| O que e cobrado | Quanto |
|---|---|
| **Glue Data Catalog** | $1.00 por 100k objetos de metadata por mes |
| **Crawler** | $0.10 por crawler por hora (nao usado aqui) |
| **ETL Job** | Baseado em DPU (Data Processing Units) - nao usado aqui |

Para as 4 tabelas neste tutorial, o custo sera ~$0.00.

### Glue Crawler vs Schema Manual

Neste tutorial, vamos criar o schema **manualmente** (via Glue API/CLI). Isso da controle total sobre os nomes de colunas e tipos.

A alternativa seria usar o **Glue Crawler**, que:
1. Scaneia os arquivos no S3
2. Infere o schema baseado no conteudo (tipos, delimitadores)
3. Cria a tabela automaticamente

**Vantagens do Crawler**: Mais rapido, menos trabalho manual.
**Desvantagens do Crawler**: Pode errar tipos, nao permite configuracao avancada de SerDe.

**Voce vai aprender como o Crawler funcionana na secao [Alternativa: Glue Crawler](#16-alternativa-glue-crawler), mas vamos usar schema manual neste tutorial para ter controle total.

---

## 5. Pre-requisitos

| Requisito | Como verificar | Como instalar |
|---|---|---|
| AWS CLI v2 | `aws --version` | `../install_aws_pre_req/install.sh` |
| Terraform | `terraform version` | `../install_aws_pre_req/install.sh` |
| Credenciais AWS | `aws sts get-caller-identity` | `../install_aws_pre_req/setup_aws_credentials.sh` |

### Sobre o ambiente Learner Lab

Este tutorial foi desenhado para o **AWS Academy Learner Lab**. Restricoes importantes:

| Restricao | Valor | Impacto no tutorial |
|---|---|---|
| Regioes | us-east-1, us-west-2 | Usamos us-east-1 |
| Servicos | Glue, Athena, S3 | Todos suportados |
| Roles | LabRole | Ja pre-criada, usaremos para permissoes |

---

## 6. Estrutura do Projeto

```
tutoriais/aws-athena-glue-tutorial/
├── TUTORIAL.md                   # Este arquivo (passo a passo detalhado)
├── QUICK_TUTORIAL.md             # Guia rapido (comandos automaticos)
├── data/                         # Dados CSV de entrada
│   ├── clientes.csv              #   15 clientes (id, nome, email, cidade, estado)
│   ├── produtos.csv              #   10 produtos (id, nome, categoria, preco)
│   └── vendas.csv                #   30 vendas (id, id_cliente, id_produto, qtd, data)
├── terraform/                    # Infraestrutura como codigo
│   ├── versions.tf               #   Provider AWS versao 5.x
│   ├── main.tf                   #   S3 bucket + uploads dos CSVs
│   └── outputs.tf                #   Outputs (bucket name, paths)
└── scripts/
    └── destroy.sh                # Descomissionamento completo
```

---

## 7. Dados de Exemplo

Os dados sao exatamente os mesmos do tutorial anterior:

### clientes.csv

| Coluna | Tipo | Exemplo |
|---|---|---|
| id_cliente | INT | 1 |
| nome | STRING | Joao Silva |
| email | STRING | joao@email.com |
| cidade | STRING | Sao Paulo |
| estado | STRING | SP |

15 clientes em 12 estados diferentes.

### produtos.csv

| Coluna | Tipo | Exemplo |
|---|---|---|
| id_produto | INT | 1 |
| nome_produto | STRING | Smartphone X |
| categoria | STRING | Eletronicos |
| preco | DOUBLE | 2500.00 |

10 produtos em 5 categorias: **Eletronicos, Acessorios, Vestuario, Esportes, Livros**.

### vendas.csv

| Coluna | Tipo | Exemplo |
|---|---|---|
| id_venda | INT | 1 |
| id_cliente | INT | 1 |
| id_produto | INT | 1 |
| quantidade | INT | 2 |
| data_venda | STRING | 2026-01-15 |

30 vendas realizadas em Janeiro e Fevereiro de 2026.

### Query que vamos executar

```sql
SELECT c.estado, p.categoria,
       round(SUM(v.quantidade * p.preco), 2) AS total_vendido,
       COUNT(*) AS num_vendas
FROM vendas v
JOIN clientes c ON v.id_cliente = c.id_cliente
JOIN produtos p ON v.id_produto = p.id_produto
GROUP BY c.estado, p.categoria
ORDER BY total_vendido DESC;
```

**O que a query faz:**
1. **JOIN**: Junta vendas com clientes (pelo id_cliente) e com produtos (pelo id_produto)
2. **GROUP BY**: Agrupa os resultados por estado e categoria
3. **Agregacao**: Calcula o total vendido (quantidade x preco) e conta quantas vendas cada grupo teve
4. **ORDER BY**: Ordena do maior valor para o menor
5. O resultado e gravado como um arquivo CSV na pasta `results/resultado_vendas/`

---

## 8. Passo 1 — Configurar credenciais AWS

### Verificar conectividade

```bash
aws sts get-caller-identity
```

**Resultado esperado:**

```json
{
    "UserId": "AROA...",
    "Account": "XXXXXXXXXXXX",
    "Arn": "arn:aws:sts::XXXXXXXXXXXX:assumed-role/voclabs/user..."
}
```

> 📸 **Print sugerido — `images/01-aws-sts-get-caller-identity.png`:** Terminal mostrando o JSON com `UserId`, `Account` e `Arn`.
>
> ![Resultado do aws sts get-caller-identity](images/01-aws-sts-get-caller-identity.png)

Se retornar erro, configure as credenciais:
```bash
../install_aws_pre_req/setup_aws_credentials.sh
```

---

## 9. Passo 2 — Terraform: Criar infraestrutura S3

### Navegar ate a pasta do projeto

```bash
cd ~/Documents/Big\ Data/tutoriais/aws-athena-glue-tutorial/terraform
```

### Inicializar o Terraform

```bash
terraform init
```

**O que acontece:**
1. Terraform baixa o provider AWS (plugin que conversa com a API da AWS)
2. Inicializa o ambiente para execucao

**Resultado esperado:**

```
Initializing the backend...
Initializing provider plugins...
- Installing hashicorp/aws v5.x.x...
Terraform has been successfully initialized!
```

> 📸 **Print sugerido — `images/02-terraform-init.png`:** Terminal com a mensagem `Terraform has been successfully initialized!`.
>
> ![Resultado do terraform init](images/02-terraform-init.png)

### Ver o que sera criado (plan)

```bash
terraform plan
```

**Resultado esperado (resumido):**

```
Terraform will perform the following actions:

  # aws_s3_bucket.athena_lab will be created
  # aws_s3_object.clientes will be created
  # aws_s3_object.produtos will be created
  # aws_s3_object.vendas will be created

Plan: 4 to add, 0 to change, 0 to destroy.
```

> 📸 **Print sugerido — `images/03-terraform-plan.png`:** Terminal mostrando os 4 recursos que serao criados.
>
> ![Resultado do terraform plan](images/03-terraform-plan.png)

### Criar a infraestrutura

```bash
terraform apply -auto-approve
```

> **Seguranca**: Isso cria recursos na AWS. O bucket S3 custa poucos centavos por mes.

**O que acontece durante o apply:**

```
[0s]   Terraform comeca a criar recursos
        - Cria o bucket S3 com nome unico (baseado no Account ID)
        - Faz upload dos 3 CSVs para data/

[~5s]  Terraform conclui
        - Bucket criado com force_destroy=true
        - 3 arquivos CSV uploaded
```

> 📸 **Print sugerido — `images/04-terraform-apply.png`:** Terminal mostrando o final do `terraform apply` com `Apply complete! Resources: 4 added`.
>
> ![Resultado do terraform apply](images/04-terraform-apply.png)

### Ver os outputs

```bash
terraform output
```

**Resultado esperado:**

```
athena_work_group = "primary"
s3_bucket = "XXXXXXXXXXXX-athena-lab"
s3_bucket_arn = "arn:aws:s3:::XXXXXXXXXXXX-athena-lab"
s3_data_path = "s3://XXXXXXXXXXXX-athena-lab/data/"
s3_results_path = "s3://XXXXXXXXXXXX-athena-lab/results/resultado_vendas/"
```

> 📸 **Print sugerido — `images/05-terraform-output.png`:** Terminal mostrando os outputs (`s3_bucket`, `s3_data_path`, `s3_results_path`).
>
> ![Resultado do terraform output](images/05-terraform-output.png)

Salve o nome do bucket:
```bash
BUCKET=$(terraform output -raw s3_bucket)
echo $BUCKET
```

---

## 10. Passo 3 — Verificar S3

### Listar objetos no bucket

```bash
BUCKET=$(terraform output -raw s3_bucket)
aws s3 ls s3://${BUCKET}/data/
```

**Resultado esperado:**

```
                           PRE clientes/
                           PRE produtos/
                           PRE vendas/
```

> 📸 **Print sugerido — `images/06-s3-ls-data.png`:** Terminal mostrando as 3 pastas em `data/` (`clientes/`, `produtos/`, `vendas/`).
>
> ![Resultado do aws s3 ls data](images/06-s3-ls-data.png)

### Verificar conteudo de cada pasta

```bash
aws s3 ls s3://${BUCKET}/data/clientes/
aws s3 ls s3://${BUCKET}/data/produtos/
aws s3 ls s3://${BUCKET}/data/vendas/
```

**Resultado esperado:**

```
2026-05-21 ...        384  clientes.csv
2026-05-21 ...        274  produtos.csv
2026-05-21 ...        763  vendas.csv
```

Agora o S3 esta configurado corretamente com os 3 CSVs. O proximo passo e criar o schema no Glue.

> 📸 **Print sugerido — `images/07-s3-console-bucket.png`:** Console AWS S3 mostrando o bucket criado com as pastas principais.
>
> ![Console S3 com bucket criado](images/07-s3-console-bucket.png)

> 📸 **Print sugerido — `images/08-s3-console-data-folder.png`:** Console AWS S3 mostrando o conteudo da pasta `data/`.
>
> ![Console S3 com pasta data](images/08-s3-console-data-folder.png)

---

## 11. Passo 4 — Criar tabelas no Glue Data Catalog

### O que e o Glue Data Catalog?

O Glue Data Catalog e um **repositorio central de metadata**. Ele armazena informacoes sobre suas tabelas: nome da tabela, nome das colunas, tipos de dados, localizacao no S3, formato do arquivo, etc.

**Importante**: O Glue **nao armazena os dados**. Os dados ficam no S3. O Glue so guarda o **schema** (metadata) que descreve como os dados estao estruturados.

### Estrutura hierarquica do Glue

```
Glue Data Catalog
└── Database: "athena_lab"
    ├── Table: "clientes"     (schema: id_cliente, nome, email, cidade, estado)
    ├── Table: "produtos"    (schema: id_produto, nome_produto, categoria, preco)
    ├── Table: "vendas"      (schema: id_venda, id_cliente, id_produto, quantidade, data_venda)
    └── Table: "resultado_vendas"  (schema: estado, categoria, total_vendido, num_vendas)
```

### Passo 4.1 — Criar o Database no Glue

Primeiro, crie um database para organizar as tabelas:

```bash
aws glue create-database \
    --database-input '{"Name": "athena_lab", "Description": "Database para tutorial AWS Athena + Glue"}' \
    --region us-east-1
```

**O que acontece:**
- Cria um database chamado `athena_lab` no Glue Data Catalog
- Databases sao containers logicos para agrupar tabelas

**Resultado esperado:** Nenhum erro (silent success).

> 📸 **Print sugerido — `images/09-glue-create-database.png`:** Terminal sem erros apos executar o comando de criacao do database.
>
> ![Criacao do database no Glue](images/09-glue-create-database.png)

**Verificar:**
```bash
aws glue get-database --name athena_lab --region us-east-1
```

> 📸 **Print sugerido — `images/10-glue-get-database.png`:** Terminal com o JSON retornado por `aws glue get-database`.
>
> ![Resultado do get-database](images/10-glue-get-database.png)

### Passo 4.2 — Criar tabela de Clientes

Agora vamos criar a tabela `clientes` com schema fixo:

```bash
BUCKET=$(terraform output -raw s3_bucket)

aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "clientes",
        "Description": "Tabela de clientes",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/clientes/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {
                    "skip.header.line.count": "1",
                    "field.delim": ","
                }
            },
            "Columns": [
                {"Name": "id_cliente", "Type": "int"},
                {"Name": "nome", "Type": "string"},
                {"Name": "email", "Type": "string"},
                {"Name": "cidade", "Type": "string"},
                {"Name": "estado", "Type": "string"}
            ]
        }
    }' \
    --region us-east-1
```

**Explicacao de cada campo:**

| Campo | Valor | Explicacao |
|---|---|---|
| `Name` | `"clientes"` | Nome da tabela |
| `Location` | `s3://BUCKET/data/clientes/` | Onde os dados estao no S3 |
| `InputFormat` | `TextInputFormat` | Formato de leitura (texto puro) |
| `OutputFormat` | `HiveIgnoreKeyTextOutputFormat` | Formato de escrita |
| `SerdeInfo.SerializationLibrary` | `LazySimpleSerDe` | Como serializar/deserializar o CSV |
| `SerdeInfo.Parameters.field.delim` | `,` | Delimitador de colunas e virgula |
| `SerdeInfo.Parameters.skip.header.line.count` | `1` | Pula a primeira linha (cabecalho) |
| `Columns` | lista de colunas | Schema da tabela: nome e tipo |

**Verificar:**
```bash
aws glue get-table --database-name athena_lab --name clientes --region us-east-1
```

> 📸 **Print sugerido — `images/11-glue-tabela-clientes.png`:** Terminal com o JSON da tabela `clientes`.
>
> ![Resultado do get-table clientes](images/11-glue-tabela-clientes.png)

### Passo 4.3 — Criar tabela de Produtos

```bash
BUCKET=$(terraform output -raw s3_bucket)

aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "produtos",
        "Description": "Tabela de produtos",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/produtos/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {
                    "skip.header.line.count": "1",
                    "field.delim": ","
                }
            },
            "Columns": [
                {"Name": "id_produto", "Type": "int"},
                {"Name": "nome_produto", "Type": "string"},
                {"Name": "categoria", "Type": "string"},
                {"Name": "preco", "Type": "double"}
            ]
        }
    }' \
    --region us-east-1
```

**Verificar:**
```bash
aws glue get-table --database-name athena_lab --name produtos --region us-east-1
```

> 📸 **Print sugerido — `images/12-glue-tabela-produtos.png`:** Terminal com o JSON da tabela `produtos`.
>
> ![Resultado do get-table produtos](images/12-glue-tabela-produtos.png)

### Passo 4.4 — Criar tabela de Vendas

```bash
BUCKET=$(terraform output -raw s3_bucket)

aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "vendas",
        "Description": "Tabela de vendas",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/data/vendas/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {
                    "skip.header.line.count": "1",
                    "field.delim": ","
                }
            },
            "Columns": [
                {"Name": "id_venda", "Type": "int"},
                {"Name": "id_cliente", "Type": "int"},
                {"Name": "id_produto", "Type": "int"},
                {"Name": "quantidade", "Type": "int"},
                {"Name": "data_venda", "Type": "string"}
            ]
        }
    }' \
    --region us-east-1
```

**Verificar:**
```bash
aws glue get-table --database-name athena_lab --name vendas --region us-east-1
```

> 📸 **Print sugerido — `images/13-glue-tabela-vendas.png`:** Terminal com o JSON da tabela `vendas`.
>
> ![Resultado do get-table vendas](images/13-glue-tabela-vendas.png)

### Passo 4.5 — Listar todas as tabelas

```bash
aws glue get-tables --database-name athena_lab --region us-east-1
```

**Resultado esperado:**

```json
{
    "TableList": [
        {"Name": "clientes", ...},
        {"Name": "produtos", ...},
        {"Name": "vendas", ...}
    ]
}
```

> 📸 **Print sugerido — `images/14-glue-list-tables.png`:** Terminal listando as 3 tabelas (`clientes`, `produtos`, `vendas`).
>
> ![Resultado do get-tables](images/14-glue-list-tables.png)

Agora o Glue Data Catalog tem o schema das 3 tabelas de entrada. O proximo passo e executar a query no Athena.

> 📸 **Print sugerido — `images/15-glue-console-database.png`:** Console AWS Glue mostrando o database `athena_lab`.
>
> ![Console Glue com database](images/15-glue-console-database.png)

> 📸 **Print sugerido — `images/16-glue-console-tables.png`:** Console AWS Glue mostrando as 3 tabelas criadas.
>
> ![Console Glue com tabelas](images/16-glue-console-tables.png)

---

## 12. Passo 5 — Executar queries no Athena

### O que e o Athena Query Editor?

O Athena tem um Query Editor no console AWS onde voce pode escrever e executar SQL. Neste tutorial, vamos usar a **AWS CLI** para executar queries via linha de comando.

### Passo 5.1 — Verificar tables disponiveis no Athena

Antes de executar a query principal, vamos verificar que o Athena consegue ver as tabelas que criamos no Glue:

```bash
aws athena list-table-metadata \
    --catalog-name AwsDataCatalog \
    --database-name athena_lab \
    --region us-east-1
```

**Resultado esperado:** Lista das 3 tabelas (clientes, produtos, vendas).

> 📸 **Print sugerido — `images/17-athena-list-tables.png`:** Terminal com as 3 tabelas visiveis no Athena.
>
> ![Resultado do list-table-metadata](images/17-athena-list-tables.png)

### Passo 5.2 — Executar a query principal

Vamos executar a query que faz JOIN das 3 tabelas e gera o resultado:

```bash
BUCKET=$(terraform output -raw s3_bucket)

aws athena start-query-execution \
    --query-string "
        SELECT c.estado, p.categoria,
               round(SUM(v.quantidade * p.preco), 2) AS total_vendido,
               COUNT(*) AS num_vendas
        FROM athena_lab.vendas v
        JOIN athena_lab.clientes c ON v.id_cliente = c.id_cliente
        JOIN athena_lab.produtos p ON v.id_produto = p.id_produto
        GROUP BY c.estado, p.categoria
        ORDER BY total_vendido DESC
    " \
    --result-configuration "OutputLocation=s3://${BUCKET}/results/resultado_vendas/" \
    --work-group primary \
    --region us-east-1
```

**O que acontece:**

| Parametro | Valor | Explicacao |
|---|---|---|
| `query-string` | SQL completo | Query que sera executada |
| `result-configuration.OutputLocation` | `s3://BUCKET/results/resultado_vendas/` | Onde o resultado sera gravado |
| `work-group` | `primary` | Grupo de trabalho padrao do Athena |

**Resultado esperado:**

```json
{
    "QueryExecutionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

> 📸 **Print sugerido — `images/18-athena-query-execution-id.png`:** Terminal com o `QueryExecutionId` retornado.
>
> ![Resultado do start-query-execution](images/18-athena-query-execution-id.png)

O `QueryExecutionId` e o ID unico da execucao. Guarde para verificar o status.

### Passo 5.3 — Verificar status da query

```bash
aws athena get-query-execution \
    --query-execution-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
    --region us-east-1
```

**Resultado esperado (query em execucao):**

```json
{
    "QueryExecution": {
        "QueryExecutionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "Query": "SELECT c.estado ...",
        "StatementType": "DML",
        "ResultConfiguration": {
            "OutputLocation": "s3://BUCKET/results/resultado_vendas/"
        },
        "QueryExecutionContext": {
            "Database": "athena_lab"
        },
        "Status": {
            "State": "RUNNING",
            "SubmissionDateTime": "2026-05-21T...",
            "CompletionDateTime": null
        }
    }
}
}

```

> 📸 **Print sugerido — `images/19-athena-status-running.png`:** Terminal mostrando `State: RUNNING` no status da query.
>
> ![Status RUNNING da query Athena](images/19-athena-status-running.png)

Aguarde ate o `State` mudar para `SUCCEEDED`. Para queries pequenas como esta, geralmente leva **5-15 segundos**.

**Verificar estado repetidamente ate SUCCEEDED:**

```bash
for i in {1..30}; do
    STATE=$(aws athena get-query-execution \
        --query-execution-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
        --query 'QueryExecution.Status.State' \
        --output text \
        --region us-east-1)
    echo "Estado: $STATE"
    if [ "$STATE" == "SUCCEEDED" ]; then
        break
    fi
    if [ "$STATE" == "FAILED" ]; then
        echo "Query falhou!"
        aws athena get-query-execution \
            --query-execution-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
            --query 'QueryExecution.Status.StateChangeReason' \
            --output text \
            --region us-east-1
        break
    fi
    sleep 2
done
```

> 📸 **Print sugerido — `images/20-athena-status-succeeded.png`:** Terminal mostrando `State: SUCCEEDED` apos a finalizacao da query.
>
> ![Status SUCCEEDED da query Athena](images/20-athena-status-succeeded.png)

**Estados possiveis:**
- `QUEUED`: Query na fila, aguardandoexecucao
- `RUNNING`: Query em execucao
- `SUCCEEDED`: Query concluida com sucesso
- `FAILED`: Query falhou (verificar mensagem de erro)
- `CANCELLED`: Query cancelada pelo usuario

> 📸 **Print sugerido — `images/21-athena-console-query-editor.png`:** Console AWS Athena (Query Editor) com a query SQL preenchida.
>
> ![Athena Query Editor](images/21-athena-console-query-editor.png)

> 📸 **Print sugerido — `images/22-athena-console-query-results.png`:** Console AWS Athena mostrando os resultados da query executada.
>
> ![Athena Query Results](images/22-athena-console-query-results.png)

---

## 13. Passo 6 — Ver os Resultados

### Listar arquivos no S3

```bash
BUCKET=$(terraform output -raw s3_bucket)
aws s3 ls s3://${BUCKET}/results/resultado_vendas/
```

**Resultado esperado:**

```
                           PRE _SUCCESS
2026-05-21 ...          0  _SUCCESS
2026-05-21 ...        576  a1b2c3d4-e5f6-7890-abcd-ef1234567890.csv
```

> 📸 **Print sugerido — `images/23-s3-resultado-arquivo.png`:** Terminal mostrando o arquivo CSV de resultado no caminho `results/resultado_vendas/`.
>
> ![Arquivo CSV de resultado no S3](images/23-s3-resultado-arquivo.png)

O Athena criou um arquivo CSV com o resultado. O nome do arquivo e o ID da query.

### Ver o conteudo do resultado

```bash
aws s3 cp s3://${BUCKET}/results/resultado_vendas/a1b2c3d4-e5f6-7890-abcd-ef1234567890.csv - \
    | column -t -s','
```

**Resultado esperado:**

```
estado  categoria     total_vendido  num_vendas
SP     Eletronicos   9500.0         2
PA     Eletronicos   4500.0         1
BA     Eletronicos   4500.0         1
PR     Eletronicos   3200.0         2
RJ     Eletronicos   2500.0         1
DF     Eletronicos   2500.0         1
MG     Livros        600.0          1
...
```

> 📸 **Print sugerido — `images/24-resultado-csv-conteudo.png`:** Terminal mostrando o conteudo formatado do CSV de resultado.
>
> ![Conteudo do CSV de resultado](images/24-resultado-csv-conteudo.png)

### Ver resultado pelo Athena (via CLI)

```bash
aws athena get-query-results \
    --query-execution-id a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
    --region us-east-1
```

**Resultado esperado:**

```json
{
    "ResultSet": {
        "Rows": [
            {"Data": [{"VarCharValue": "SP"}, {"VarCharValue": "Eletronicos"}, ...]},
            {"Data": [{"VarCharValue": "PA"}, {"VarCharValue": "Eletronicos"}, ...]},
            ...
        ]
    }
}
```

> 📸 **Print sugerido — `images/25-s3-console-resultado.png`:** Console AWS S3 mostrando o arquivo CSV de resultado na pasta `results/resultado_vendas/`.
>
> ![Console S3 com arquivo de resultado](images/25-s3-console-resultado.png)

### Download local

```bash
mkdir -p results
aws s3 sync s3://${BUCKET}/results/resultado_vendas/ results/
ls -la results/
```

### Criar tabela de resultado no Glue (opcional)

Se quiser consultar o resultado como uma tabela no futuro, crie uma tabela Glue apontando para a pasta de resultados:

```bash
BUCKET=$(terraform output -raw s3_bucket)

aws glue create-table \
    --database-name athena_lab \
    --table-input '{
        "Name": "resultado_vendas",
        "Description": "Resultado da query: total vendido por estado e categoria",
        "StorageDescriptor": {
            "Location": "s3://'"${BUCKET}"'/results/resultado_vendas/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerDeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {
                    "field.delim": ","
                }
            },
            "Columns": [
                {"Name": "estado", "Type": "string"},
                {"Name": "categoria", "Type": "string"},
                {"Name": "total_vendido", "Type": "double"},
                {"Name": "num_vendas", "Type": "bigint"}
            ]
        },
        "TableType": "EXTERNAL_TABLE"
    }' \
    --region us-east-1
```

Agora voce pode fazer novas queries no Athena sobre o resultado:

```sql
SELECT * FROM resultado_vendas LIMIT 10;
```

---

## 14. Passo 7 — Descomissionamento

> **IMPORTANTE**: Sempre destrua os recursos ao terminar. O bucket S3 custa poucos centavos por mes, mas e boa pratica limpar.

### Via script automatizado (recomendado)

```bash
cd ~/Documents/Big\ Data/tutoriais/aws-athena-glue-tutorial
./scripts/destroy.sh
```

### Via Terraform (manual)

```bash
cd ~/Documents/Big\ Data/tutoriais/aws-athena-glue-tutorial/terraform
terraform destroy -auto-approve
```

> 📸 **Print sugerido — `images/26-terraform-destroy.png`:** Terminal mostrando `Destroy complete! Resources: 4 destroyed`.
>
> ![Resultado do terraform destroy](images/26-terraform-destroy.png)

### Via comandos AWS (se o Terraform falhar)

```bash
# 1. Deletar tabelas do Glue
aws glue delete-table --database-name athena_lab --name clientes --region us-east-1
aws glue delete-table --database-name athena_lab --name produtos --region us-east-1
aws glue delete-table --database-name athena_lab --name vendas --region us-east-1
aws glue delete-table --database-name athena_lab --name resultado_vendas --region us-east-1

# 2. Deletar database do Glue
aws glue delete-database --name athena_lab --region us-east-1

# 3. Deletar bucket S3 (esvaziar primeiro)
BUCKET=$(terraform output -raw s3_bucket)
aws s3 rm s3://${BUCKET} --recursive
aws s3 rb s3://${BUCKET}
```

### Verificar que tudo foi removido

```bash
# Verificar buckets
aws s3 ls | grep athena-lab

# Verificar databases Glue
aws glue get-databases --region us-east-1
```

> 📸 **Print sugerido — `images/27-glue-databases-empty.png`:** Terminal mostrando que nao ha mais databases no Glue apos o descomissionamento.
>
> ![Glue sem databases](images/27-glue-databases-empty.png)

---

## 15. Custos e Orcamento

### Estimativa de custo por sessao

| Recurso | Quantidade | Custo | Tempo estimado | Custo |
|---|---|---|---|---|
| S3 storage | ~2 KB | ~$0.00 | Mes | ~$0.00 |
| Glue Data Catalog | 4 tabelas | $1.00/100k objetos | Mes | ~$0.00 |
| Athena (dados escaneados) | ~2 KB | $5.00/TB | 1 query | ~$0.00 |
| **Total por sessao** | | | | | **~$0.00** |

### Dicas para economizar

1. **Nao ha EC2s para parar** — Athena e Glue sao serverless, nao ha instancias para gerenciar
2. **Resultados no S3** — resultados persistem apos queries
3. **Use compressao** — arquivos .gz no S3 rendem menos dados escaneados
4. **_partitioning** — particionar dados por data/estado reduz dados escaneados

---

## 16. Alternativa: Glue Crawler

### O que e um Glue Crawler?

O Glue Crawler e um componente que **automatiza a descoberta de schema**. Em vez de criar cada tabela manualmente, o Crawler:
1. Scaneia os arquivos em uma pasta do S3
2. Infer o tipo de cada coluna analisando amostras
3. Cria a tabela no Glue automaticamente

### Como funcionana um Crawler

**Passo a passo:**

1. Criar um Crawler apontando para a pasta `s3://BUCKET/data/clientes/`
2. Executar o Crawler
3. O Crawler le os primeiros 1000 rows do arquivo CSV
4. Infere: `id_cliente` (int), `nome` (string), `email` (string), `cidade` (string), `estado` (string)
5. Cria a tabela `clientes` no Glue com o schema inferido

### Comandos para criar um Crawler (exemplo para clientes)

```bash
BUCKET=$(terraform output -raw s3_bucket)

# 1. Criar o Crawler
aws glue create-crawler \
    --name athena-lab-clientes-crawler \
    --role LabRole \
    --database-name athena_lab \
    --targets '{"S3Targets": [{"Path": "s3://'"${BUCKET}"'/data/clientes/"}]}' \
    --schema-change-policy '{"UpdateBehavior": "LOG", "DeleteBehavior": "DEPRECATE_IN_DATABASE"}' \
    --region us-east-1

# 2. Executar o Crawler
aws glue start-crawler --name athena-lab-clientes-crawler --region us-east-1

# 3. Verificar status
aws glue get-crawler --name athena-lab-clientes-crawler --region us-east-1
# Aguardar ate State mudar para READY
```

### Vantagens do Crawler

- **Rapido**: Nao precisa especificar cada coluna manualmente
- **Boa para exploracao**: Ideal quando voce esta descobrindo a estrutura dos dados

### Desvantagens do Crawler

- **Menos controle**: Pode errar tipos (ex: inferir `string` onde deveria ser `int`)
- **Sem configuracao avancada**: Nao permite setting manual de SerDe, separadores customizados
- **Mesmas limitacoes**: Se seus dados tem quirks (ex: valores nulos como "N/A"), o Crawler pode nao lidar bem

### Recomendacao

Para **producao** ou quando voce precisa de controle exato do schema, **crie as tabelas manualmente** (como fizemos neste tutorial).

Para **experimentacao** ou datasets desconhecidos, o Crawler e uma otima ferramenta paraDiscovery rapido.

---

## 17. Troubleshooting

| Problema | Causa | Solucao |
|---|---|---|
| `terraform init` falha | Provider AWS nao baixa | Verifique conexao com internet |
| `terraform apply` erro de permissao | LabRole nao tem permissao | Roles pre-criadas no Learner Lab |
| `aws glue create-table` falha | tabela ja existe | Delete primeiro: `aws glue delete-table` |
| Query Athena `FAILED` | Erro de SQL | Verifique: `aws athena get-query-execution --query-execution-id XXX --query Status.StateChangeReason` |
| `Access Denied` no S3 | Bucket policy restritiva | O Learner Lab tem permissao via LabRole |
| Tabela nao aparece no Athena | Database errado | Verifique `--database-name athena_lab` |
| Resultado vazio | Pasta errada ou query sem resultados | Verifique `aws s3 ls s3://BUCKET/results/resultado_vendas/` |
| Orcamento esgotado | Budget do lab excedido | Nao ha recuperacao. Monitore o budget |

### Verificar permissoes do Glue

```bash
aws glue get-database --database-name athena_lab --region us-east-1
```

### Verificar permissoes do Athena

```bash
aws athena list-query-executions --work-group primary --region us-east-1
```

---

## Anexo: Queries Uteis no Athena

```sql
-- Ver todas as tabelas
SHOW TABLES IN athena_lab;

-- Ver schema de uma tabela
DESCRIBE athena_lab.clientes;

-- Ver sample de dados
SELECT * FROM athena_lab.clientes LIMIT 5;

-- Contar registros
SELECT COUNT(*) AS total_vendas FROM athena_lab.vendas;

-- Contar por estado
SELECT estado, COUNT(*) AS total FROM athena_lab.clientes GROUP BY estado;

-- Query completa (a mesma do tutorial)
SELECT c.estado, p.categoria,
       round(SUM(v.quantidade * p.preco), 2) AS total_vendido,
       COUNT(*) AS num_vendas
FROM athena_lab.vendas v
JOIN athena_lab.clientes c ON v.id_cliente = c.id_cliente
JOIN athena_lab.produtos p ON v.id_produto = p.id_produto
GROUP BY c.estado, p.categoria
ORDER BY total_vendido DESC;
```

---

## Anexo: Comparacao Athena vs EMR Hive

| Aspecto | AWS Athena + Glue | AWS EMR + Hive |
|---|---|---|
| **Modelo** | Serverless | Cluster gerenciado |
| **Compute** | Multitenant (AWS gerencia) | EC2 instances (voce paga) |
| **Custo** | Pay-per-query ($5/TB) | Pay-per-hour (instancia) |
| **Setup time** | Segundos | 10-15 minutos |
| **Escalabilidade** | Automatica | Manual (adicionar nodes) |
| **HiveQL/SQL** | SQL Athena (baseado em Presto) | HiveQL (MapReduce/Tez) |
| **Schema** | Glue Data Catalog | Hive Metastore (no cluster) |
| **Dados** | Lidos diretamente do S3 | Lidos do S3 ou HDFS |
| **HDFS** | Nao suportado | Suportado (local ao cluster) |
| **Melhor para** | BI ad-hoc, queries leve | ETL pesado, jobs complexos |