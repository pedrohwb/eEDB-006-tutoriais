# Guia de Prints — AWS Athena + Glue Tutorial

Este diretório armazena as capturas de tela do tutorial.

## Como tirar os prints

- **macOS**: Cmd + Shift + 4 (seleção de área)
- **Windows**: Win + Shift + S (Snipping Tool)
- **Linux**: Flameshot ou Print Screen

## Lista de Prints

| Arquivo | Passo | Descrição |
|---|---|---|
| 01-aws-sts-get-caller-identity.png | Passo 1 | Terminal: resultado do `aws sts get-caller-identity` com `UserId`, `Account` e `Arn` |
| 02-terraform-init.png | Passo 2 | Terminal: `terraform init` com “Terraform has been successfully initialized!” |
| 03-terraform-plan.png | Passo 2 | Terminal: `terraform plan` mostrando 4 recursos a criar |
| 04-terraform-apply.png | Passo 2 | Terminal: `terraform apply` com “Apply complete! Resources: 4 added” |
| 05-terraform-output.png | Passo 2 | Terminal: `terraform output` mostrando bucket e paths |
| 06-s3-ls-data.png | Passo 3 | Terminal: `aws s3 ls s3://${BUCKET}/data/` com clientes/, produtos/ e vendas/ |
| 07-s3-console-bucket.png | Passo 3 | Console S3: bucket criado com as pastas |
| 08-s3-console-data-folder.png | Passo 3 | Console S3: conteúdo da pasta `data/` |
| 09-glue-create-database.png | Passo 4 | Terminal: criação do database Glue sem erros |
| 10-glue-get-database.png | Passo 4 | Terminal: JSON do `aws glue get-database` |
| 11-glue-tabela-clientes.png | Passo 4 | Terminal: JSON da tabela `clientes` |
| 12-glue-tabela-produtos.png | Passo 4 | Terminal: JSON da tabela `produtos` |
| 13-glue-tabela-vendas.png | Passo 4 | Terminal: JSON da tabela `vendas` |
| 14-glue-list-tables.png | Passo 4 | Terminal: listagem das 3 tabelas no Glue |
| 15-glue-console-database.png | Passo 4 | Console Glue: database `athena_lab` |
| 16-glue-console-tables.png | Passo 4 | Console Glue: tabelas `clientes`, `produtos` e `vendas` |
| 17-athena-list-tables.png | Passo 5 | Terminal: `list-table-metadata` mostrando as 3 tabelas |
| 18-athena-query-execution-id.png | Passo 5 | Terminal: `start-query-execution` com `QueryExecutionId` |
| 19-athena-status-running.png | Passo 5 | Terminal: status da query com `State: RUNNING` |
| 20-athena-status-succeeded.png | Passo 5 | Terminal: status da query com `State: SUCCEEDED` |
| 21-athena-console-query-editor.png | Passo 5 | Console Athena: Query Editor com SQL |
| 22-athena-console-query-results.png | Passo 5 | Console Athena: resultados da query |
| 23-s3-resultado-arquivo.png | Passo 6 | Terminal: CSV de resultado em `results/resultado_vendas/` |
| 24-resultado-csv-conteudo.png | Passo 6 | Terminal: conteúdo formatado do CSV de resultado |
| 25-s3-console-resultado.png | Passo 6 | Console S3: arquivo CSV de resultado |
| 26-terraform-destroy.png | Passo 7 | Terminal: `terraform destroy` com “Destroy complete! Resources: 4 destroyed” |
| 27-glue-databases-empty.png | Passo 7 | Terminal: `aws glue get-databases` sem databases restantes |
